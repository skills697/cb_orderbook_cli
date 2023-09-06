package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"time"

	_ "github.com/lib/pq"
)

type DBConnInfo struct {
	database string
	connect  string
	user     string
	password string
}

type Order struct {
	Price  float64
	Volume float64
	Count  float64
}

type OrderBook struct {
	Bids        []Order     `json:"bids"`
	Asks        []Order     `json:"asks"`
	Sequence    int64       `json:"sequence"`
	AuctionMode bool        `json:"auction_mode"`
	Auction     interface{} `json:"auction"`
	Time        string      `json:"time"`
}

func (o *Order) UnmarshalJSON(data []byte) error {
	var v []interface{}
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	o.Price, _ = strconv.ParseFloat(v[0].(string), 64)
	o.Volume, _ = strconv.ParseFloat(v[1].(string), 64)
	o.Count, _ = v[2].(float64)
	return nil
}

type Candle struct {
	Time       int64
	PriceLow   float64
	PriceHigh  float64
	PriceOpen  float64
	PriceClose float64
	Volume     float64
}

func (c *Candle) UnmarshalJSON(data []byte) error {
	var v []interface{}
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	//unmarshal wants to recognize the timestamp as float instead of uint64
	tval, _ := v[0].(float64)
	c.Time = int64(tval)
	c.PriceLow, _ = v[1].(float64)
	c.PriceHigh, _ = v[2].(float64)
	c.PriceOpen, _ = v[3].(float64)
	c.PriceClose, _ = v[4].(float64)
	c.Volume, _ = v[5].(float64)
	return nil
}

type SnapshotOrders struct {
	Side     string
	MinPrice float64
	MaxPrice float64
	Volume   float64
}

var DBInfo *DBConnInfo

/**
* main()
* Initialize and run Ticker
 */
func main() {
	tradingPair := "BTC-USD"

	/*** DATABASE CONNECTION INFO ***/
	// connFileName := os.Getenv("CB_DB_CONN_FILE")
	// if connFileName == "" {
	// 	log.Fatal("CB_DB_CONN_FILE environment variable not set")
	// }
	//
	// connDbName := os.Getenv("CB_DB_NAME")
	// if connDbName == "" {
	// 	log.Fatal("CB_DB_NAME environment variable not set")
	// }
	//
	// DBInfo = GetConnectionInfo(connFileName, connDbName)
	DBInfo = GetConnectionInfo("C:\\PS\\go\\cb_cli\\dbconn.txt", "coinbaseg")

	fmt.Println("Skipping Sleep due to test of process")

	// Get the current timestamp from remote ticker - used to lookup last candle data
	currentTime, tickerTimeErr := getCurrentTickerTimestamp(tradingPair)
	CheckError(tickerTimeErr)

	currentCandle, candleErr := getCurrentCandle(tradingPair, currentTime)
	CheckError(candleErr)

	timeDiff := currentTime.Unix() - currentCandle.Time
	timeSleep := (300 - timeDiff) + 15
	fmt.Printf("CTime[%v], TTime[%v], Diff[%v]\n", currentCandle.Time, currentTime.Unix(), timeDiff)

	fmt.Printf("Sleeping %v Seconds\n", timeSleep)

	// Sleep until next candle tick
	time.Sleep(time.Duration(timeSleep) * time.Second)

	// run initial gather API process
	gatherDataAndStore(tradingPair)

	// begin 5 min intervals
	FetchTicker := time.NewTicker(5 * time.Minute)

	for range FetchTicker.C {
		gatherDataAndStore(tradingPair)
	}
}

func gatherDataAndStore(tradingPair string) {
	timeStart := time.Now()

	fmt.Println("------------------------------------------")
	fmt.Println("-     BEGIN RUNNING API FETCH PROCESS : ", timeStart)

	// Timestamp
	currentTime, tickerTimeErr := getCurrentTickerTimestamp(tradingPair)
	CheckError(tickerTimeErr)

	if currentTime != nil {
		// Next Candle
		currentCandle, candleErr := getCurrentCandle(tradingPair, currentTime)
		CheckError(candleErr)

		// Current Orderbook
		orderBook, err := getOrderBookData("BTC-USD", 3)
		CheckError(err)

		fmt.Println("Recieved ", len(orderBook.Bids), " bids")
		fmt.Println("Recieved ", len(orderBook.Asks), " asks")

		if len(orderBook.Bids) <= 0 && len(orderBook.Asks) <= 0 {
			fmt.Println("Found no orders")
		}

		db, dbErr := sql.Open("postgres", DbConnectionString(DBInfo))
		if dbErr != nil {
			log.Fatal("Error: Could not connect to database")
		}
		defer db.Close()

		snapId := 0
		snapErr := db.QueryRow("INSERT INTO snapshot (trading_pair, unix_ts_start, price_low, price_high, price_open, price_close, volume) "+
			"VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING snapshot_id;",
			tradingPair,
			currentCandle.Time,
			currentCandle.PriceLow,
			currentCandle.PriceHigh,
			currentCandle.PriceOpen,
			currentCandle.PriceClose,
			currentCandle.Volume).Scan(&snapId)
		CheckError(snapErr)

		buyOrders, sellOrders := groupSnapshotOrders(orderBook.Bids, orderBook.Asks)

		buyCount, buyOrdErr := storeSnapshotOrders(db, int64(snapId), buyOrders)
		CheckError(buyOrdErr)

		sellCount, sellOrdErr := storeSnapshotOrders(db, int64(snapId), sellOrders)
		CheckError(sellOrdErr)

		fmt.Printf("Snapshot %v Processed Successfully! Buys[%v] Sells[%v]\n", snapId, buyCount, sellCount)
	}
	fmt.Println("-     COMPLETED API FETCH PROCESS : Duration ", time.Since(timeStart))
	fmt.Printf("------------------------------------------\n\n")

}

func groupSnapshotOrders(buyOrders []Order, sellOrders []Order) ([]SnapshotOrders, []SnapshotOrders) {
	//Assumes the array of orders is descending in price (as the coinbase api provides)
	maxBuy := buyOrders[0].Price
	groupSize := snapshotGroupSize(maxBuy)

	buySnaps := []SnapshotOrders{}
	groupMin := 0.0
	groupMax := groupMin + groupSize
	currSnap := SnapshotOrders{
		Side:     "buy",
		MinPrice: groupMin,
		MaxPrice: groupMax,
		Volume:   0.0,
	}

	for i := len(buyOrders) - 1; i >= 0; i-- {
		if buyOrders[i].Price > currSnap.MaxPrice {
			buySnaps = append(buySnaps, currSnap)
			for buyOrders[i].Price > currSnap.MaxPrice && currSnap.MaxPrice < maxBuy {
				groupMin = groupMax
				groupMax += groupSize
				if groupMax > maxBuy {
					groupMax = maxBuy
				}
				currSnap = SnapshotOrders{
					Side:     "buy",
					MinPrice: groupMin,
					MaxPrice: groupMax,
					Volume:   0.0,
				}
				if buyOrders[i].Price > currSnap.MaxPrice {
					buySnaps = append(buySnaps, currSnap)
				}
			}
			currSnap.Volume += buyOrders[i].Volume
		} else {
			currSnap.Volume += buyOrders[i].Volume
		}
	}
	buySnaps = append(buySnaps, currSnap)

	sellSnaps := []SnapshotOrders{}
	minSell := sellOrders[0].Price
	maxSell := groupMin + groupSize
	if maxSell < minSell {
		for maxSell < minSell {
			maxSell += groupSize
		}
	}
	currSnap = SnapshotOrders{
		Side:     "sell",
		MinPrice: minSell,
		MaxPrice: maxSell,
		Volume:   0.0,
	}
	for i := 0; i < len(sellOrders); i++ {
		if sellOrders[i].Price > currSnap.MaxPrice {
			sellSnaps = append(sellSnaps, currSnap)
			//TODO - figure out an improved max range for sell order cutoff (can go to infinity)
			for sellOrders[i].Price > currSnap.MaxPrice && len(sellSnaps) < 127 {
				minSell = maxSell
				maxSell += groupSize
				currSnap = SnapshotOrders{
					Side:     "sell",
					MinPrice: minSell,
					MaxPrice: maxSell,
					Volume:   0.0,
				}
				if sellOrders[i].Price > currSnap.MaxPrice {
					sellSnaps = append(sellSnaps, currSnap)
				}
			}
			currSnap.Volume += sellOrders[i].Volume
			if len(sellSnaps) > 127 {
				break
			}
		} else {
			currSnap.Volume += sellOrders[i].Volume
		}
	}
	sellSnaps = append(sellSnaps, currSnap)

	//fmt.Println("Top Buy: ", buySnaps[len(buySnaps)-1])
	//fmt.Println("Bottom Sell: ", sellSnaps[0])

	return buySnaps, sellSnaps
}

func snapshotGroupSize(maxBuy float64) float64 {
	// We want to capture 100 evenly sized groups of buy orders
	// Find a group size rounded to largest possible decimal
	sTo := maxBuy / 100.0
	sFrom := maxBuy / 99.0
	sDiff := sFrom - sTo
	i := 1.0
	if sDiff > 10.0 {
		for (sDiff / i) > 10.0 {
			i *= 10.0
		}
	} else {
		for (sDiff / i) < 1.0 {
			i /= 10.0
		}
	}
	return math.Ceil(sTo/i) * i
}

func storeSnapshotOrders(db *sql.DB, snapId int64, orders []SnapshotOrders) (int, error) {
	success := 0
	for _, order := range orders {
		orderResult, orderResErr := db.Exec("INSERT INTO orders (snapshot_id, side, min_price, max_price, total_size) VALUES ($1, $2, $3, $4, $5)",
			snapId,
			order.Side,
			order.MinPrice,
			order.MaxPrice,
			order.Volume)
		if orderResErr != nil {
			return success, orderResErr
		}

		rowCount, rowCountErr := orderResult.RowsAffected()
		if rowCountErr != nil {
			return success, rowCountErr
		}
		if rowCount < 1 {
			return success, errors.New("Error: Order Data !")
		}
		success++
	}
	return success, nil
}

func getCurrentTickerTimestamp(tradingPair string) (*time.Time, error) {
	/* TICKER DATA */
	tickerData, tickerErr := getProductTicker(tradingPair)
	if tickerErr != nil {
		return nil, tickerErr
	}

	tickerTime, timeErr := time.Parse(time.RFC3339, tickerData["time"].(string))
	if timeErr != nil {
		return nil, timeErr
	}

	return &tickerTime, nil
}

func getCurrentCandle(tradingPair string, tickerTime *time.Time) (*Candle, error) {
	/* CANDLE DATA */
	cLen := 0
	cCnt := 0
	var candleRes *Candle
	for cLen <= 0 && cCnt < 10 {
		candleData, candleErr := getProductCandles(tradingPair, (tickerTime.Unix() - 300), (tickerTime.Unix() + 1), 300)
		if candleErr != nil {
			return nil, candleErr
		}

		cLen += len(candleData)
		cCnt++

		if cCnt >= 9 {
			return nil, errors.New("No candle found from remote")
		} else if cLen <= 0 {
			fmt.Println("Waiting for refresh of candle data. Sleeping 5 seconds")
			time.Sleep(5 * time.Second)
		}

		candleRes = &candleData[0]
	}

	return candleRes, nil
}

func DbConnectionString(dbConn *DBConnInfo) string {
	return "postgres://" +
		dbConn.user + ":" +
		dbConn.password + "@" +
		dbConn.connect + "/" +
		dbConn.database + "?sslmode=disable"
}

func getOrderBookData(tradingPair string, level int) (*OrderBook, error) {
	url := "https://api.exchange.coinbase.com/products/" + tradingPair + "/book?level=" + strconv.Itoa(level)
	var orderBook OrderBook

	reqErr := getHttpRequest(url, &orderBook)

	return &orderBook, reqErr
}

func getProductCandles(tradingPair string, startTime int64, endTime int64, granularity int) ([]Candle, error) {
	url := "https://api.exchange.coinbase.com/products/" + tradingPair + "/candles"
	vcount := 0
	if startTime > 0 || endTime > 0 || granularity > 0 {
		url += "?"
		if startTime > 0 {
			url += "start=" + strconv.FormatInt(startTime, 10)
			vcount++
		}
		if endTime > 0 {
			if vcount > 0 {
				url += "&"
			}
			url += "end=" + strconv.FormatInt(endTime, 10)
			vcount++
		}
		if granularity > 0 {
			if vcount > 0 {
				url += "&"
			}
			url += "granularity=" + strconv.Itoa(granularity)
			vcount++
		}
	}

	var candles []Candle
	//fmt.Println("Sending Candle Request:")
	//fmt.Println(url)
	reqErr := getHttpRequest(url, &candles)

	return candles, reqErr
}

func getProductTicker(tradingPair string) (map[string]interface{}, error) {
	url := "https://api.exchange.coinbase.com/products/" + tradingPair + "/ticker"
	var tickerData map[string]interface{}

	reqErr := getHttpRequest(url, &tickerData)

	return tickerData, reqErr
}

func getHttpRequest(url string, results interface{}) error {

	resp, err := http.Get(url)
	if err != nil {
		return fmt.Errorf("Error sending http request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		fmt.Println("$$$ Recieved Irregular response code from remote server:")
		fmt.Println("$$$ ", url, " - Response: ", resp.StatusCode)
	}

	byteResult, readErr := io.ReadAll(resp.Body)
	if readErr != nil {
		return fmt.Errorf("Error reading http response: %v", err)
	}

	unjsonErr := json.Unmarshal([]byte(byteResult), results)
	if unjsonErr != nil {
		return fmt.Errorf("Error parsing json response: %v", err)
	}

	return nil
}

func GetConnectionInfo(filename string, database string) *DBConnInfo {
	fileOpen, err := os.Open(filename)
	CheckError(err)
	defer fileOpen.Close()
	scanner := bufio.NewScanner(fileOpen)
	var res DBConnInfo
	noDbFound := true

	for scanner.Scan() {
		inputLine := scanner.Text()

		if inputLine[:len(database)] == database {
			noDbFound = false

			rxp, errxp := regexp.Compile(
				"(?P<database>\\w*)=(?P<user>\\w*)/(?P<password>\\w*)@(?P<connection>\\w*).*$")
			CheckError(errxp)

			m := rxp.FindStringSubmatch(inputLine)

			for i, nam := range rxp.SubexpNames() {
				//fmt.Println(i, " : ", nam)
				if i <= 0 || i >= len(m) {
					continue
				} else if nam == "user" {
					//fmt.Println("User is ", m[i])
					res.user = m[i]
				} else if nam == "password" {
					//fmt.Println("Password Found")
					res.password = m[i]
				} else if nam == "database" {
					//fmt.Println("Database is ", m[i])
					res.database = m[i]
				} else if nam == "connection" {
					//fmt.Println("Connection is ", m[i])
					res.connect = m[i]
				}
			}
		}
	}

	CheckError(scanner.Err())
	if noDbFound {
		log.Fatalf("Failed to find \"%v\" in \"%v\"\n", database, filename)
	}

	return &res
}

func CheckError(e error) {
	if e != nil {
		log.Fatal(e)
		panic(e)
	}
}
