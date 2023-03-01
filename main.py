from ibapi.client import EClient
from ibapi.wrapper import EWrapper
import subprocess
import json
from ibapi.contract import Contract
from ibapi.order import Order
import threading
import time
import csv
import requests

def run_loop():
    app.run()

class IBapi(EWrapper, EClient):
    def __init__(self):
        EClient.__init__(self, self)
        self.nextorderId = None

    def nextValidId(self, orderId: int):
        super().nextValidId(orderId)
        self.nextorderId = orderId
        print('The next valid order id is: ', self.nextorderId)

    def orderStatus(self, orderId, status, filled, remaining, avgFullPrice, permId, parentId, lastFillPrice, clientId,
                    whyHeld, mktCapPrice):
        print('orderStatus - orderid:', orderId, 'status:', status, 'filled', filled, 'remaining', remaining,
              'lastFillPrice', lastFillPrice)
        data = {
            "orderId": orderId,
            "status": status,
            "filled": filled,
            "remaining": remaining,
            "avgFullPrice": avgFullPrice,
            "permId": permId,
            "parentId": parentId,
            "lastFillPrice": lastFillPrice,
            "clientId": clientId,
            "whyHeld": whyHeld,
            "mktCapPrice": mktCapPrice
        }
        url = "https://6f7a7af5483995333cbb1092029788d3.m.pipedream.net"
        headers = {"Authorization": "360d90f4549def76cc1e370e71832b67"}
        requests.post(url, headers=headers, json=data)

    def openOrder(self, orderId, contract, order, orderState):
        print('openOrder id:', orderId, contract.symbol, contract.strike, contract.right, contract.secType,
              contract.lastTradeDateOrContractMonth, '@', contract.exchange, ':', order.action,
              order.orderType, order.totalQuantity, orderState.status)
        # Open the CSV file in "append" mode
        with open('openOrder.csv', 'a', newline='') as file:
            # Create a CSV writer object
            writer = csv.writer(file)
            # Write a new row to the CSV file
            row = [orderId,
                   contract.symbol,
                   contract.strike,
                   contract.right,
                   contract.secType,
                   contract.lastTradeDateOrContractMonth,
                   contract.exchange,
                   order.action,
                   order.orderType,
                   order.totalQuantity,
                   orderState.status]
            writer.writerow(row)

    def execDetails(self, reqId, contract, execution):
        print('Order Executed: ', reqId, contract.symbol, contract.strike, contract.right, contract.secType,
              contract.lastTradeDateOrContractMonth, contract.currency, execution.execId,
              execution.orderId, execution.shares, execution.lastLiquidity)
        data = {
            "reqId": reqId,
            "symbol": contract.symbol,
            "Strike:": contract.strike,
            "Right:": contract.right,
            "secType": contract.secType,
            "currency": contract.currency,
            "execId": execution.execId,
            "orderId": execution.orderId,
            "shares": execution.shares,
            "lastLiquidity": execution.lastLiquidity
        }
        url = "https://eoqlqyok7ol5g9t.m.pipedream.net"
        headers = {"Authorization": "360d90f4549def76cc1e370e71832b67"}
        requests.post(url, headers=headers, json=data)

# Create an instance of the IBapi class
app = IBapi()
app.connect('127.0.0.1', 4002, 123)
app.nextorderId = None

# Start the socket in a thread
api_thread = threading.Thread(target=run_loop, daemon=True)
api_thread.start()

# Check if the API is connected via orderid
while True:
    if isinstance(app.nextorderId, int):
        print('connected')
        break
    else:
        print('waiting for connection')
        time.sleep(1)

# Listen for incoming webhook data
while True:
    url = "https://api.pipedream.com/sources/dc_2Eu7X4x/sse"
    auth_header = "Authorization: Bearer 360d90f4549def76cc1e370e71832b67"
    curl_cmd = ["curl", "-s", "-N", "-H", auth_header, url]
    proc = subprocess.Popen(curl_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    for line in proc.stdout:
    # Ignore any SSE data that isn't JSON-formatted
     if line.startswith(b"data: {"):
        # Strip the "data: " prefix from the line and parse the JSON-formatted data
        event_data = json.loads(line[6:])
        # Process the event data as needed
        data = event_data
        body = data['event']['body']
        symbol_fields = body.split(',')

        # Open the CSV file in "append" mode
        with open('filename.csv', 'a', newline='') as file:
            # Create a CSV writer object
            writer = csv.writer(file)
            # Write a new row to the CSV file
            row = [symbol_fields[0],
                   symbol_fields[1],
                   symbol_fields[2],
                   symbol_fields[3],
                   symbol_fields[4],
                   symbol_fields[5],
                   symbol_fields[6],
                   symbol_fields[7]]
            writer.writerow(row)

        with open('filename.csv', 'r') as file:
            csv_reader = csv.reader(file)
            data = list(csv_reader)

        second_to_last_row = data[-2]
        if symbol_fields[6] == second_to_last_row[6]:
            continue
        else:
            def options_order_1(symbol):
                # Create SELL Contract Object
                contract_1 = Contract()
                contract_1.symbol = second_to_last_row[0]
                contract_1.secType = second_to_last_row[1]
                contract_1.exchange = second_to_last_row[2]
                contract_1.currency = second_to_last_row[3]
                contract_1.lastTradeDateOrContractMonth = second_to_last_row[4]
                contract_1.strike = second_to_last_row[5]
                contract_1.right = second_to_last_row[6]
                contract_1.multiplier = second_to_last_row[7]
                return contract_1

            # Define the BUY Contract and Order Objects
            def options_order_2(symbol):
                # Create BUY Contract Object
                contract_2 = Contract()
                contract_2.symbol = symbol_fields[0]
                contract_2.secType = symbol_fields[1]
                contract_2.exchange = symbol_fields[2]
                contract_2.currency = symbol_fields[3]
                contract_2.lastTradeDateOrContractMonth = symbol_fields[4]
                contract_2.strike = symbol_fields[5]
                contract_2.right = symbol_fields[6]
                contract_2.multiplier = symbol_fields[7]
                return contract_2

            # Create SELL Order Object
            contract_1 = options_order_1(second_to_last_row[0])
            order_1 = Order()
            order_1.action = 'SELL'
            order_1.totalQuantity = 1
            order_1.orderType = 'MKT'
            order_1.eTradeOnly = ''
            order_1.firmQuoteOnly = ''
            # Place the SELL Order
            app.placeOrder(app.nextorderId, contract_1, order_1)

            # Wait for the first order to be filled before placing the second order
            time.sleep(3)

            # Place BUY order
             # Create BUY Order Object
            contract_2 = options_order_2(symbol_fields[0])
            order_2 = Order()
            order_2.action = 'BUY'
            order_2.totalQuantity = 1
            order_2.orderType = 'MKT'
            order_2.eTradeOnly = ''
            order_2.firmQuoteOnly = ''
            # Place the BUY Order
            app.nextorderId += 1
            app.placeOrder(app.nextorderId, contract_2, order_2)
            app.nextorderId += 1
        time.sleep(3)