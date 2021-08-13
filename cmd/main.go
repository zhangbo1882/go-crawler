package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/pprof"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/go-redis/redis/v8"
	"github.com/oliveagle/jsonpath"
	"k8s.io/klog/v2"
)

const (
	HOST         = "https://www.arrow.com/en/products"
	ROOT         = "https://www.arrow.com"
	REDIS_SERVER = "127.0.0.1:6666"
	GO_NUMBER    = 100
)

type Item struct {
	Id           string
	PartNumber   string
	Category     string
	Manufacturer string
	Price        string
}

type ItemConfig struct {
	url    string
	pages  uint32
	lock   sync.Mutex
	wg     sync.WaitGroup
	items  []Item
	client *redis.Client
	ctx    *context.Context
	parent *Spider
}
type Spider struct {
	itemConfig  []ItemConfig
	redisClient *redis.Client
	wg          sync.WaitGroup
	ctx         context.Context
	failNumber  int
	totalNumber int
}

func (item Item) MarshalBinary() ([]byte, error) {
	return json.Marshal(item)
}

func (item Item) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, item)
}

func startDebugHttpServer() {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	server := &http.Server{
		Addr:    "0.0.0.0:8888",
		Handler: mux,
	}
	klog.Fatal(server.ListenAndServe())
}

func init() {
	klog.InitFlags(nil)
	flag.Set("logtostderr", "false")
	flag.Set("log_file", "crawler.log")
	flag.Set("v", "2")
	flag.Parse()
	klog.Flush()
	go startDebugHttpServer()
}

func (s *ItemConfig) Goquery(index int) {
	var url string
	if index == 0 {
		url = s.url
	} else {
		url = fmt.Sprintf("%s?page=%d", s.url, index)
	}
	doc, err := goquery.NewDocument(url)
	if err != nil {
		klog.Fatalf("Goquery new document fails")
	}
	klog.Infof("Begin to visting %s", url)
	err = s.Parse(doc, index)
	if err != nil {
		if index == 0 {
			klog.Infof("Failed to Visting %s, err: %v", url, err)
			s.parent.failNumber++
		}
	}
}

func i2s(v interface{}) string {
	switch t := v.(type) {
	case string:
		return t
	}
	klog.V(6).Infof("type: %v", reflect.TypeOf(v))
	return ""
}
func i2f(v interface{}) float64 {
	switch t := v.(type) {
	case float64:
		return t
	}
	klog.V(6).Infof("type: %v", reflect.TypeOf(v))
	return 0
}

func (s *ItemConfig) Parse(doc *goquery.Document, index int) error {
	defer func() {
		if index == 0 {
			//	s.parent.wg.Done()
		} else {
			s.wg.Done()
		}
	}()
	data := doc.Find("#arrow-state")
	rawdataString := data.Text()
	if rawdataString == "" {
		klog.Errorf("data: %v", doc.Text())
		return fmt.Errorf("Can not get source code")
	}
	dataString := strings.ReplaceAll(rawdataString, "&q;", "\"")
	var f interface{}
	if err := json.Unmarshal([]byte(dataString), &f); err != nil {
		return fmt.Errorf("Json convert fails, err: %v, host: %v", err, s.url)
	}
	if index == 0 {
		pageNumber, err := jsonpath.JsonPathLookup(f, "$.jss.sitecore.route.placeholders.arrow-main[2].placeholders.plp-details[0].placeholders.product-line-search[0].fields.firstBeSearchResults.resultsMetadata.totalPageCount")
		if err != nil {
			klog.Fatalf("Json path lookup fails, err: %v", err)
		}
		pages := i2f(pageNumber)
		klog.Infof("pageNumber: %v", pages)
		s.wg.Add(int(pages) - 1)
		i := 1
		for ; i < int(pages); i++ {
			go s.Goquery(i + 1)
		}
	}

	t, err := jsonpath.JsonPathLookup(f, "$.jss.sitecore.route.placeholders.arrow-main[2].placeholders.plp-details[0].placeholders.product-line-search[0].fields.firstBeSearchResults.results.[:]")
	if err != nil {
		return fmt.Errorf("jsonpath fails")
	}

	switch v := t.(type) {
	case []interface{}:
		for _, v1 := range v {
			var item Item
			var price interface{}
			id, _ := jsonpath.JsonPathLookup(v1, "$.partId")
			partNumber, _ := jsonpath.JsonPathLookup(v1, "$.partNumber")
			manufacturer, _ := jsonpath.JsonPathLookup(v1, "$.manufacturer")
			category, _ := jsonpath.JsonPathLookup(v1, "$.category")
			priceList, _ := jsonpath.JsonPathLookup(v1, "$.priceBands")
			switch v2 := priceList.(type) {
			case []interface{}:
				if len(v2) > 0 {
					price, _ = jsonpath.JsonPathLookup(v2[0], "$.displayPrice")
				}
			}

			item.Id = i2s(id)
			item.PartNumber = i2s(partNumber)
			item.Category = i2s(category)
			item.Manufacturer = i2s(manufacturer)
			item.Price = i2s(price)
			s.lock.Lock()
			//	s.items = append(s.items, item)
			err := s.client.SAdd(*s.ctx, item.Category, item.Id).Err()
			klog.V(6).Infof("Add id to category %s, err: %v", item.Category, err)
			err = s.client.SAdd(*s.ctx, item.Id, item).Err()
			klog.V(6).Infof("Update id %s, err: %v", item.Id, err)
			s.lock.Unlock()
		}
	}
	if index == 0 {
		klog.Infof("Page %d wait", index)
		s.wg.Wait()
		klog.Infof("All pages done for %v", s.url)
	}
	return nil
}

func (s *Spider) Init() {
	s.ctx = context.Background()
	s.redisClient = redis.NewClient(&redis.Options{
		Addr:     REDIS_SERVER,
		Password: "zhangbo",
		DB:       0,
	})
	if s.redisClient == nil {
		klog.Fatalf("Redis c kloglient failure")
	}
	klog.Infof("Init success")
}

func (s *Spider) GenerateConfig() {
	urls, err := s.redisClient.SMembers(s.ctx, "itemURL").Result()
	if err != nil {
		klog.Fatalf("Redis get data failure, err: %v", err)
	}
	for _, url := range urls {
		item := ItemConfig{
			url:    url,
			client: s.redisClient,
			ctx:    &s.ctx,
			parent: s,
		}
		s.itemConfig = append(s.itemConfig, item)
	}
	klog.V(6).Infof("v:%v", s.itemConfig)

}

func (s *Spider) Start() {
	//s.wg.Add(len(s.itemConfig))
	s.totalNumber = len(s.itemConfig)
	defer func() {
		klog.Infof("Total URL: %d", s.totalNumber)
		klog.Infof("Failed URL: %d", s.failNumber)
	}()
	rand.Seed(86)
	for i := 0; i < s.totalNumber; i++ {
		index := rand.Intn(s.totalNumber)
		s.itemConfig[index].Goquery(0)
		time.Sleep(5 * time.Second)
	}
	//s.wg.Wait()
}

func (s *Spider) GetURL() {
	doc, err := goquery.NewDocument(HOST)
	if err != nil {
		klog.Fatalf("Goquery new document fails")
	}
	doc.Find(".CategoryListings-subItems-item").Each(func(i int, sel *goquery.Selection) {
		href, _ := sel.Find("a").Attr("href")
		klog.V(6).Infof("href: %v", href)
		s.redisClient.SAdd(s.ctx, "itemURL", ROOT+href)
	})

}

func main() {
	spider := Spider{}
	spider.Init()
	//	spider.GetURL()
	spider.GenerateConfig()
	spider.Start()
}
