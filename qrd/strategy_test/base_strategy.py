import pandas as  pd
from abc import ABC, abstractmethod
from collections import deque
from sortedcontainers import SortedDict
from qrd.data.utils import extract_messages
from qrd.strategy_test.order_book import *

class OrderRequest:
    def __init__(self, asset, ts):
        self.asset = asset
        self.ts = ts
        self.latent_ts=ts+pd.Timedelta(nanoseconds=500)

class AddRequest(OrderRequest):
    def __init__(self, asset, ts, side, price, quantity):
        super().__init__(asset, ts)
        self.side = side
        self.price = price
        self.quantity = quantity

class DeleteRequest(OrderRequest):
    def __init__(self, asset, ts ,order_id, side):
        super().__init__(asset, ts)
        self.order_id = order_id
        self.side = side

class ExecuteRequest(OrderRequest):
    def __init__(self, asset, ts, side, quantity):
        super().__init__(asset, ts)
        self.side = side
        self.quantity = quantity
    
class Order:
    """
    Represents an order with separate timestamps for when the order was sent and when it was received.
    The orders that we receive from the market are represented by the Message class.
    """
    def __init__(self, sent_ts, received_ts, msg_type, side, price, qty, order_id, que_loc, asset_name=None):
        self.sent_ts = sent_ts
        self.received_ts = received_ts
        self.type = msg_type
        self.side = side
        self.price = price
        self.qty = qty
        self.id = order_id
        self.que_loc = que_loc
        self.asset = asset_name

    def __str__(self):
        """
        Returns a string representation of the Order object
        """
        return f"Order(Sent Timestamp: {self.sent_ts}, Received Timestamp: {self.received_ts}, Type: {self.type}, " \
               f"Side: {self.side}, Price: {self.price}, Quantity: {self.qty}, Order ID: {self.id}, " \
               f"Queue Location: {self.que_loc}, Asset: {self.asset})"



class BaseStrategy(ABC):

    def __init__(self, data : pd.DataFrame, assets : list[str], cash : float, inventory : dict[str, float]):
        """
        Initialize the strategy with a list of assets, an initial cash balance, and an initial inventory.
        """
        self.assets = assets
        self.cash = cash
        self.inventory = inventory
        self.lobs : dict[str, OrderBook] = {}
        self.data=data
        self.current_ts=0
        self.requestsQ : dict[str,deque[OrderRequest]] = {}#asset:queue[request]
        self.orders_by_id : dict[str,dict[str, dict[int,Order]]]={}#asset:side:order_id:order
        self.orders_by_price : dict[str,dict[str, SortedDict[float, deque[str]]]]={}#asset:side:price:queue[order_id]
        #keeping track of the orders via this id
        self.id_counter=0
        for asset in assets:
            self.reset_asset(asset)
    
    def reset_asset(self, asset : str):
        self.lobs[asset] = OrderBook(asset)
        self.requestsQ[asset]=deque()
        self.orders_by_id[asset]={}
        self.orders_by_id[asset]['B']={}
        self.orders_by_id[asset]['S']={}
        self.orders_by_price[asset]={}
        self.orders_by_price[asset]['B']=SortedDict(OrderBook._return_neg)
        self.orders_by_price[asset]['S']=SortedDict()

    def get_current_ts(self):
        return self.current_ts
    
    def get_cash(self):
        return self.cash
    
    def get_inventory(self, asset : str=None):
        if asset is None:
            return self.inventory
        else:
            return self.inventory[asset]
    
    def get_orders_at_px(self, asset : str, side : str, price : float):
        Q = self.orders_by_price[asset][side].get(price,[])
        list_of_orders=deque()
        for order_id in Q:
            list_of_orders.append(self.orders_by_id[asset][side][order_id])
        return list_of_orders

    
    def get_all_orders(self, asset : str, side : str):
        return self.orders_by_id[asset][side].values()
    
    def get_order(self, order_id : int, asset : str, side : str):
        return self.orders_by_id[asset][side].get(order_id)
        
    
    def __handle_requests__(self, asset):
        while len(self.requestsQ[asset]) != 0 and self.requestsQ[asset][0].latent_ts<=self.current_ts:
            request = self.requestsQ[asset].popleft()
            if isinstance(request, AddRequest):
                self.__handle_add_request__(request)
            elif isinstance(request, DeleteRequest):
                self.__handle_delete_request__(request)
            elif isinstance(request, ExecuteRequest):
                self.__handle_market_order_request__(request)
            else:
                raise ValueError('Invalid request type.')

    def __handle_add_request__(self, request : AddRequest):
        que_loc=self.get_book(request.asset).get_Q_size(request.side,request.price)
        order=Order(request.ts, request.latent_ts,'A', request.side, request.price, request.quantity, self.id_counter, que_loc, request.asset)
        self.id_counter+=1
        self.orders_by_id[request.asset][request.side][order.id]=order
        self.orders_by_price[request.asset][request.side].setdefault(request.price,deque()).append(order.id)
        self.on_transaction(order, 'A')
    
    def __handle_delete_request__(self, request : DeleteRequest):
        try:
            order=self.orders_by_id[request.asset][request.side][request.order_id]
            self.orders_by_price[request.asset][request.side][order.price].remove(request.order_id)
            del self.orders_by_id[request.asset][request.side][request.order_id]
            self.on_transaction(order, 'D')
        except KeyError:
            print('Order not found. Probably already executed.')

    def __handle_market_order_request__(self, request : ExecuteRequest):       
        vwap=0
        qty=0
        if request.side=='B':
            while request.quantity-qty>0:
                available_qty = self.lobs[request.asset].get_best_ask()[1]
                if available_qty==None:
                    print('No available order in the market.')
                    return
                px, lots=self.lobs[request.asset].get_best_ask()[0], min(available_qty, request.quantity)
                if px==None:
                    print('No more orders to execute.')
                    return
                self.cash-=px*lots
                vwap+=px*lots
                qty+=lots
                self.inventory[request.asset]+=lots
        else:
            while request.quantity-qty>0:
                available_qty = self.lobs[request.asset].get_best_bid()[1]
                if available_qty==None:
                    print('No available order in the market.')
                    return
                px, lots=self.lobs[request.asset].get_best_bid()[0], min(available_qty, request.quantity)
                self.cash+=px*lots
                vwap+=px*lots
                qty+=lots
                self.inventory[request.asset]-=lots
        vwap/=qty
        #creating a market order 'M' with the vwap and the quantity
        order=Order(request.ts, self.get_current_ts(),'M', request.side, vwap, qty, self.id_counter, 0, request.asset)
        self.id_counter+=1
        self.on_transaction(order, 'M', qty)
    
    def __handle_messages__(self, Q_loc, message : Message ):
        """
        this method updates the queue locations of the orders and deletes the orders that are executed after calling on_transaction.
        """
        if message.type=='D':
            self.__handle_delete_message__(Q_loc,message)
        elif message.type=='A':
            pass
            #self.__handle_add__(asset, px, sum_qty, q_loc, side)
        elif message.type=='E':
            self.__handle_exec_message__(Q_loc,message)
        elif message.type=='O':#market event
            if message.flag in ['P_GUNSONU','P_ACS_EMR_TP_PY_EIY']:
                self._reset_levels_(message)
            
        
    def __handle_delete_message__(self,Q_loc,message : Message):
        Q=self.orders_by_price[message.asset][message.side].get(message.price)
        if Q is None or len(Q)==0:
            return
        tempQ=deque()
        while len(Q)>0:
            next_id=Q.popleft()
            our_order=self.get_order(next_id,message.asset,message.side)
            our_q_loc=our_order.que_loc
            if our_q_loc > Q_loc and our_q_loc>0:
                our_order.que_loc-=1
            tempQ.append(next_id)
        self.orders_by_price[message.asset][message.side][message.price]=tempQ
    """    
    def __handle_add__(self, asset, px, sum_qty, q_loc,side):
        #if an add order of opposite side come to a price level on which we have orders with q_loc=0,
        #we should execute them, otherwise, no need to update
        opp_side='B' if side=='S' else 'S'
        Q=self.orders_by_price[asset][opp_side].get(px)
        if Q is not None and len(Q)>0:
            order=self.orders_by_id[asset][opp_side][Q[0]]                              
            self.__handle_exec_message__(asset, px, sum_qty, q_loc,'B')
    """
    def __handle_exec_message__(self,Q_loc : int, message : Message):
        #if there is an executed order on that price level, we should update the queue locations of the orders
        Q=self.get_orders_at_px(message.asset,message.side,message.price)
        if Q is None or len(Q)==0:
            return
        if Q_loc==0:#it means that the order in the book is fully executed        
            our_order_in_front=Q[0]
            if our_order_in_front.que_loc!=0:#it mens thaty our orders in the Q are behind the order that is executed
                for order in Q:
                    order.que_loc-=1
            else:#it means that our orders in the Q are in front of the order that is executed
                exec_qty=message.qty
                qty_to_be_executed=min(our_order_in_front.qty,exec_qty)
                while our_order_in_front.que_loc==0 and exec_qty>0:
                    #will call self.ontransaction
                    our_order_in_front.qty-=qty_to_be_executed
                    if message.side=='B':
                        self.cash-=message.price*qty_to_be_executed
                        self.inventory[message.asset]+=qty_to_be_executed
                    else:
                        self.cash+=message.price*qty_to_be_executed
                        self.inventory[message.asset]-=qty_to_be_executed
                    self.on_transaction(our_order_in_front,'E',qty_to_be_executed)
                    if our_order_in_front.qty==0:
                        Q.popleft()
                        self.orders_by_price[message.asset][message.side][message.price].remove(our_order_in_front.id)
                        del self.orders_by_id[message.asset][message.side][our_order_in_front.id]
                    exec_qty-=qty_to_be_executed
                    if len(Q)>0:
                        our_order_in_front=Q[0]
                    else:
                        break
        else:#it means that the order in the book is partially executed, I will adjust bthe sum qty
            pass

    def get_book(self, asset : str):
        """
        Get the order book for an asset.
        """
        return self.lobs[asset]
    
    def get_requests(self, asset : str=None):
        """
        Get the requests for an asset.
        """
        if asset is None:
            return self.requestsQ
        else:
            return self.requestsQ[asset]
    
    #These methods are used to add reequests to the queue, they are not directly directed to the lob
    def add_order(self, asset : str, side : str, price : float, quantity : int):
        """
        send an order to the order book.
        """
        if price==None or quantity==None:
            raise ValueError('Price and quantity must not be null.')
        if price<=0 or quantity<=0:
            raise ValueError('Price and quantity must be positive.')
        
        if asset not in self.assets:
            raise ValueError('Asset not found.')
        ##############################!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        self.requestsQ[asset].append(AddRequest(asset, self.current_ts, side, price, quantity))
    
    def delete_order(self, asset : str, order_id : int, side : str):
        """
        Delete an order from the order book.
        """
        if asset not in self.assets:
            raise ValueError('Asset not found.')
        
        if order_id not in self.orders_by_id[asset]['B'] and order_id not in self.orders_by_id[asset]['S']:
            raise ValueError('Order not found.')

        self.requestsQ[asset].append(DeleteRequest(asset, self.current_ts, order_id, side))
    
    def market_order(self, asset : str, side : str, quantity : int):
        """
        Execute a market order.
        """
        if asset not in self.assets:
            raise ValueError('Asset not found.')
        
        if quantity==None:
            raise ValueError('Quantity must not be null.')

        if quantity<=0:
            raise ValueError('Quantity must be positive.')

        self.requestsQ[asset].append(ExecuteRequest(asset, self.current_ts, side, quantity))


    def run(self):
        """
        Run the strategy.
        """
        for row in self.data.itertuples():
            #this tuple will be used for the update the orders and their Q locations
            msg=Message.from_tuple(row)
            book=self.lobs[msg.asset]
            self.__handle_requests__(msg.asset)
            new_Q_loc=book.on_new_message(msg)
            self.current_ts=book.get_ts()
            self.__handle_messages__(new_Q_loc,msg)
            self.on_update(msg)


    @abstractmethod 
    def on_update(self, message : Message):
        """
        this method is called per message for each asset. It is an asbstract method and should be implemented by the user.
        """
        pass
    
    @abstractmethod 
    def on_transaction(self,order : Order, event_type : str, exec_qty : int=None):
        """
        this method is called per trade for each asset. It is an abstract method and should be implemented by the user.
        Params:
        order: the order that is added, deleted or executed
        event_type: 'A', 'D' 'E' 'M' for add, delete, execute, market order(E-->execution of a limit order, M-->execution of a market order)
        """
        pass


class MyStrategy(BaseStrategy):
#    def __init__(self, data : pd.DataFrame, assets : list[str], cash : float, inventory : dict[str, float]):

    def __init__(self, data, assets):
        super().__init__(data, assets,cash=100000,inventory={'AKBNK':0})
    
    def on_update(self, message : Message):
                
        current_ts=self.get_current_ts()    
        start_time = pd.Timestamp(current_ts.date()).replace(hour=10, minute=0, second=0)
        end_time = pd.Timestamp(current_ts.date()).replace(hour=18, minute=0, second=0)

        if current_ts<start_time or current_ts>end_time:
            return
        
        for order in self.get_all_orders('AKBNK','S'):
            if len(self.orders_by_price[order.asset][order.side][order.price])>1:
                self.delete_order('AKBNK',order.id,'S')

        best_bid_px=self.get_book('AKBNK').get_best_bid()[0]
        best_ask_px=self.get_book('AKBNK').get_best_ask()[0]
        
        if self.get_inventory('AKBNK')<=0 and best_bid_px!=None:
            if len(self.get_orders_at_px('AKBNK','B',best_bid_px))==0:
                self.add_order('AKBNK','B',best_bid_px,1)
            #deleting the orders that are not in best bid
            for order in self.get_all_orders('AKBNK','B'):
                if order.price!=best_bid_px:
                    self.delete_order('AKBNK',order.id,'B')
        elif self.get_inventory('AKBNK')>0 and best_ask_px!=None:
            if len(self.get_orders_at_px('AKBNK','S',best_ask_px))==0:
                self.add_order('AKBNK','S',best_ask_px,1)
            #deleting the orders that are not in best ask
            for order in self.get_all_orders('AKBNK','S'):
                if order.price!=best_ask_px:
                    self.delete_order('AKBNK',order.id,'S')

        print(f"------{self.get_current_ts()}------")
        print(message)
        print(self.get_book('AKBNK').get_best_bid())
        print(self.get_book('AKBNK').get_best_ask())
        print(self.get_inventory('AKBNK'))
        print(self.get_cash())
        
        for order in self.get_all_orders('AKBNK','B'):
            print(order)
        for order in self.get_all_orders('AKBNK','S'):
            print(order)
                        
    def on_transaction(self, order: Order, event_type: str, exec_qty : int=None):
        #print(order, "event: ", event_type)
        pass


if __name__ == '__main__':
    from qrd.data.utils import *
    import os

    path=r"C:\Users\yusuf.bektas\Desktop\yusuf_workspace\data"
    spot=read_spot(path,'AKBNK.csv')
    msgs=extract_messages(spot.messages)
    import pandas as pd

    msgs['asset'] = "AKBNK"  
    new_column_order = ['asset'] + [col for col in msgs.columns if col != 'asset']

    msgs = msgs[new_column_order]
    my_strategy=MyStrategy(msgs,['AKBNK'])
    my_strategy.run()
            
            
