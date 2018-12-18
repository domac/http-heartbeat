package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"
)

const (
	client_num    = 100
	request_count = 20000
)

func DoRequest(httpClient *http.Client, headers map[string]string, method, loadUrl string, bodydata []byte) {
	req, err := http.NewRequest(method, loadUrl, bytes.NewBuffer(bodydata))
	if err != nil {
		fmt.Println("An error occured doing request", err)
		return
	}

	req.Header.Add("User-Agent", "hb-client")

	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		rr, ok := err.(*url.Error)
		if !ok {
			fmt.Println("An error occured doing request", err, rr)
			return
		}
	}
	if resp == nil {
		return
	}
	defer func() {
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
	}()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("An error occured reading body", err)
	}
	log.Printf("response body : %s\n", body)
	return
}

func main() {
	httpclients := []*http.Client{}
	for i := 0; i < client_num; i++ {
		httpClient := &http.Client{}
		httpclients = append(httpclients, httpClient)
	}

	var wg sync.WaitGroup

	for i := 0; i < client_num; i++ {
		wg.Add(1)
		go func(i int) {
			hc := httpclients[i]
			headers := make(map[string]string)
			bodydata := []byte{}
			loadUrl := fmt.Sprintf("http://localhost:10029/hb?cmd=5103&uid=u-%d&mid=m-%d", i*20, i)
			for j := 0; j < request_count; j++ {
				DoRequest(hc, headers, "GET", loadUrl, bodydata)
				time.Sleep(1 * time.Second)
			}
			println("\n\n>>>>> done\n\n")
			wg.Done()

		}(i)
		//time.Sleep(11 * time.Second)
	}
	wg.Wait()

}
