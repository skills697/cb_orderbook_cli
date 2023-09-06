# cb_orderbook_cli
CLI tool for capturing the coinbase order book data.

3D web viewer of the order book data: https://github.com/skills697/cb_orderbook_matrix

- repeats a 5 minute time interval (synchronized with the candle sticks updates).
- groups the buy/sell orders together into a price range. Currently capturing around 100k total orders and places them into 228 groups (100 buys, and 128 sell groups).
