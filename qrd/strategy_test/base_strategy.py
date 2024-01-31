from order_book import OrderBook
import pandas as  pd
from abc import ABC, abstractmethod
from collections import deque
from sortedcontainers import SortedDict

class OrderRequest:
    def __init__(self, asset, ts):
        self.asset = asset
        self.ts = ts

class AddRequest(OrderRequest):
    def __init__(self, asset, ts, side, price, quantity):
        super().__init__(asset, ts)
        self.side = side
        self.price = price
        self.quantity = quantity

class DeleteRequest(OrderRequest):
    def __init__(self, asset, ts ,order_id):
        super().__init__(asset, ts)
        self.order_id = order_id

class ExecuteRequest(OrderRequest):
    def __init__(self, asset, ts, side, quantity):
        super().__init__(asset, ts)
        self.side = side
        self.quantity = quantity
    
class Order:
    """
    Represents an order sent by us, the orders that we receive from the market are represented by the Message class.
    """
    def __init__(self, ts, msg_type, side, price, qty, order_id, que_loc,asset_name=None):
        self.ts = ts
        self.type = msg_type
        self.side = side
        self.price = price
        self.que_loc=que_loc
        self.qty = qty
        self.id = order_id
        self.asset = asset_name

class BaseStrategy(ABC):

    def __init__(self, data : pd.DataFrame, assets : list[str], cash : float, inventory : dict[str, float]):
        """
        Initialize the strategy with a list of assets, an initial cash balance, and an initial inventory.
        """
        self.assets = assets
        self.cash = cash
        self.inventory = inventory
        self.lobs = {}
        self.data=data
        self.current_ts=0
        #checking if the data is parallel with the assets
        if data.columns.get_level_values(0).unique().tolist() != assets:
            raise ValueError('Data columns must match assets.')
        for asset in assets:
            self.lobs[asset] = OrderBook(asset)
        self.requestsQ : dict[str,deque[OrderRequest]] = {}#asset:queue[request]
        for asset in assets:
            self.requestsQ[asset]=deque()
        
        self.orders_by_id : dict[str,dict[str, dict[int,Order]]]={}#asset:side:order_id:order
        for asset in assets:
            self.orders_by_id[asset]={}
            self.orders_by_id[asset]['B']={}
            self.orders_by_id[asset]['S']={}

        self.orders_by_price : dict[str,dict[str, SortedDict[float, deque[str]]]]={}#asset:side:price:queue[order_id]
        for asset in assets:
            self.orders_by_price[asset]={}
            self.orders_by_price[asset]['B']=SortedDict(OrderBook._return_neg)
            self.orders_by_price[asset]['S']=SortedDict()
        #keeping track of the orders via this id
        self.id_counter=0
    
    def __handle_requests__(self, prev_updates_tuple):
        if len(self.requestsQ) == 0:
            return
        while self.requestsQ[0].ts<=self.current_ts:
            request = self.requestsQ.popleft()
            if isinstance(request, AddRequest):
                self.__handle_add_request__(request)
            elif isinstance(request, DeleteRequest):
                self.__handle_delete_request__(request)
            elif isinstance(request, ExecuteRequest):
                self.__handle_market_order_request__(request)
            else:
                raise ValueError('Invalid request type.')

    def __handle_add_request__(self, request : AddRequest):
        que_loc=self.lobs[request.asset].get_que_size(request.side,request.price)
        order=Order(request.ts, 'A', request.side, request.price, request.quantity, self.id_counter, que_loc, request.asset)
        self.id_counter+=1
        self.orders_by_id[request.asset][request.side][order.id]=order
        self.orders_by_price[request.asset][request.side].setdefault(request.price,deque()).append(order.id)
        self.on_transaction(order, 'A')
    
    def __handle_delete_request__(self, request : DeleteRequest):
        try:
            order=self.orders_by_id[request.asset][request.side][request.order_id]
            del self.orders_by_id[request.asset][request.side][request.order_id]
            self.orders_by_price[request.asset][request.side][order.price].remove(request.order_id)
            self.on_transaction(order, 'D')
        except KeyError:
            print('Order not found. Probably already executed.')

    def __handle_market_order_request__(self, request : ExecuteRequest):       
        vwap=0
        qty=0
        if request.side=='B':
            while request.quantity>0:
                px, lots=self.lobs[request.asset].get_best_ask()[0], min(self.lobs[request.asset].get_best_ask()[1], request.quantity)
                self.cash-=px*lots
                vwap+=px*lots
                qty+=lots
                self.inventory[request.asset]+=lots
        else:
            while request.quantity>0:
                px, lots=self.lobs[request.asset].get_best_bid()[0], min(self.lobs[request.asset].get_best_bid()[1], request.quantity)
                self.cash+=px*lots
                vwap+=px*lots
                qty+=lots
                self.inventory[request.asset]-=lots
        vwap/=qty
        #creating a market order 'M' with the vwap and the quantity
        order=Order(request.ts, 'M', request.side, vwap, qty, self.id_counter, 0, request.asset)
        self.id_counter+=1
        self.on_transaction(order, 'M')
    
    def __handle__orders__(self, updates_tuple):
        """
        this method updates the queue locations of the orders and deletes the orders that are executed after calling on_transaction.
        """
        asset, px, sum_qty, q_loc, flag, side=updates_tuple
        if flag=='D':
            self.__handle__delete__(asset, px, sum_qty, q_loc, side)
        elif flag=='A':
            self.__handle__add__(asset, px, sum_qty, q_loc, side)
        elif flag=='E':
            self.__handle__execs__(asset, px, sum_qty, q_loc,side)
        
    def __handle__delete__(self, asset, px, sum_qty, q_loc,side):
        Q=self.orders_by_price[asset][side][px]
        tempQ=deque()
        while len(Q)>0:
            next_id=Q.popleft()
            our_q_loc=self.orders_by_id[asset][side][next_id].que_loc
            if our_q_loc>=q_loc and our_q_loc>0:
                self.orders_by_id[asset][side][next_id].que_loc-=1
            tempQ.append(next_id)
    
    def __handle__add__(self, asset, px, sum_qty, q_loc,side):
        #if an add order of opposite side come to a price level on which we have orders with q_loc=0,
        #we should execute them, otherwise, no need to update
        opp_side='B' if side=='S' else 'S'
        if self.orders_by_price[asset][opp_side].get(px) is not None and self.orders_by_price[asset][opp_side][px][0].que_loc==0:
            self.__handle__execs__(asset, px, sum_qty, q_loc,'B')

    def __handle__execs__(self, asset, px, sum_qty, q_loc,side):
        #if there is an executed order on that price level, we should update the queue locations of the orders
        Q=self.orders_by_price[asset][side][px]
        tempQ=deque()
        while len(Q)>0:
            next_id=Q.popleft()
            if self.orders_by_id[asset][side][next_id].que_loc>q_loc:
                self.orders_by_id[asset][side][next_id].que_loc-=1
                tempQ.append(next_id)
            else:
                #it means that our order is executed
                our_order=self.orders_by_id[asset][side][next_id]
                if our_order.side=='B':
                    self.cash-=our_order.price*our_order.qty
                    self.inventory[asset]+=our_order.qty
                else:
                    self.cash+=our_order.price*our_order.qty
                    self.inventory[asset]-=our_order.qty
                del self.orders_by_id[asset][side][next_id]
                self.on_transaction(our_order, 'E')
        self.orders_by_price[asset][side][px]=tempQ



    def get_book(self, asset : str):
        """
        Get the order book for an asset.
        """
        return self.lobs[asset]
    
    def send_request(self, request : OrderRequest):
        """
        Send an order request to the market.
        """
        self.requestsQ.append(request)

    def run(self):
        """
        Run the strategy.
        """
        prev_updates_tuple = None
        for row in self.data.itertuples():
            #this tuple will be used for the update the orders and their Q locations
            self.__handle_requests__(prev_updates_tuple)
            book=self.lobs[row['asset']]
            updates_tuple=book.on_new_message(row)
            self.__handle__orders__(updates_tuple)
            self.current_ts=book.get_ts()
            prev_updates_tuple=updates_tuple
            self.on_update(updates_tuple)

    @abstractmethod 
    def on_update(self, updates_tuple):
        """
        this method is called per message for each asset. It is an asbstract method and should be implemented by the user.
        """
        pass
    
    @abstractmethod 
    def on_transaction(self,order : Order, event_type : str):
        """
        this method is called per trade for each asset. It is an abstract method and should be implemented by the user.
        """
        pass

        
            