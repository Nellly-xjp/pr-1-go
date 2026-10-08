package main

import (
	"fmt"
	"net/http"
	"sync"
	"time"
)

type Result struct {
	URL        string
	StatusCode int
	Duration   time.Duration
	Err        error
}

type Checker interface {
	Check(url string) Result
}

type HTTPChecker struct {
	client *http.Client
}

func NewHTTPChecker(timeout time.Duration) *HTTPChecker {
	return &HTTPChecker{
		client: &http.Client{
			Timeout: timeout,
		},
	}
}

func (h *HTTPChecker) Check(url string) Result {
	start := time.Now()

	resp, err := h.client.Get(url)
	duration := time.Since(start)

	if err != nil {
		return Result{
			URL:        url,
			StatusCode: 0,
			Duration:   duration,
			Err:        err,
		}
	}
	defer resp.Body.Close()

	return Result{
		URL:        url,
		StatusCode: resp.StatusCode,
		Duration:   duration,
		Err:        nil,
	}
}

func runSequential(checker Checker, urls []string) (time.Duration, []Result) {
	start := time.Now()
	var results []Result

	for _, u := range urls {
		results = append(results, checker.Check(u))
	}

	return time.Since(start), results
}


func runConcurrentChannel(checker Checker, urls []string) (time.Duration, []Result) {
	start := time.Now()

	resultsChan := make(chan Result, len(urls))
	var wg sync.WaitGroup

	for _, u := range urls {
		wg.Add(1)

		
		go func(url string) {
			defer wg.Done()
			res := checker.Check(url)
			resultsChan <- res 
		}(u)
	}

	
	go func() {
		wg.Wait()
		close(resultsChan)
	}()

	var results []Result
	for res := range resultsChan {
		results = append(results, res)
	}

	return time.Since(start), results
}

func main() {
	var checker Checker = NewHTTPChecker(5 * time.Second)

	urls := []string{
		"https://go.dev",
		"https://github.com",
		"https://google.com",
		"https://invalid-domain-test-12345.org",
	}

	fmt.Println("Рівень 1 ")
	seqDuration, _ := runSequential(checker, urls)
	fmt.Printf("Завершено за: %v\n\n", seqDuration)

	fmt.Println("Рівень 3")
	concDuration, results := runConcurrentChannel(checker, urls)

	successCount := 0
	failedCount := 0

	fmt.Println("\nДеталі перевірки:")
	for _, res := range results {
		if res.Err != nil {
			failedCount++
			fmt.Printf("[ПОМИЛКА] URL: %s | Тривалість: %v | Деталі: %v\n", res.URL, res.Duration, res.Err)
		} else {
			successCount++
			fmt.Printf("[ОК]      URL: %s | Статус: %d | Тривалість: %v\n", res.URL, res.StatusCode, res.Duration)
		}
	}

	fmt.Println("\nПІДСУМКОВИЙ ЗВІТ ")
	fmt.Printf("Всього перевірено URL : %d\n", len(urls))
	fmt.Printf("Успішних запитів     : %d\n", successCount)
	fmt.Printf("Неуспішних запитів   : %d\n", failedCount)
	fmt.Println("--------------------------------------------------")

	fmt.Printf("Час послідовної роботи  : %v\n", seqDuration)
	fmt.Printf("Час паралельної роботи  : %v\n", concDuration)

	if concDuration > 0 {
		speedup := float64(seqDuration) / float64(concDuration)
		fmt.Printf("Кратне прискорення      : %.2fx\n", speedup)
	}
}