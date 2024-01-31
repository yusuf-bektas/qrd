from sortedcontainers import SortedDict
import logging
import pandas as pd
from collections import deque
        
class Message:
    """
    Represents a message about an order, its execution, or its deletion.
    """
    def __init__(self, ts, msg_type, side, price, qty, order_id, flag, bist_time=None, asset_name=None):
        self.ts = ts
        self.type = msg_type
        self.side = side
        self.price = price
        self.qty = qty
        self.id = order_id
        self.flag = flag
        self.bist_time = bist_time
        self.asset = asset_name

    @classmethod
    def from_tuple(cls, data_tuple):
        """
        Create a Message object from a tuple.
        """
        ts, msg_type, side, price, qty, order_id, flag = data_tuple
        return cls(ts, msg_type, side, price, qty, order_id, flag)

class OrderBook:
    """
    The OrderBook class represents a market order book, where orders from traders are stored.
    """

    #helper function to keep the bids in descending order
    @staticmethod
    def _return_neg(x):
        return -x

    def __init__(self,asset):
        """
        Initialize the order book.
        """
        self._reset_levels_()
        self.prev_ts=0
        self.current_ts=0
        self.asset=asset
        #for check
        """
        self.book={
            'ts':[],
            'askpx':[],
            'askqty':[],
            'askpx2':[],
            'askqty2':[],
            'askpx3':[],
            'askqty3':[],   
            'bidpx':[],
            'bidqty':[],
            'bidpx2':[],
            'bidqty2':[],
            'bidpx3':[],
            'bidqty3':[],
        }
        """

    def get_asset(self):
        return self.asset
    def get_ts(self):
        return self.current_ts
    def get_que_size(self,side,price):
        id_by_price = self.bid_ids_by_price if side == 'B' else self.ask_ids_by_price
        q=id_by_price.get(price,[])
        return len(q)

    def _reset_levels_(self):
        self.bid_ids_by_price = SortedDict(OrderBook._return_neg)#<float, queue<int>>
        self.ask_ids_by_price = SortedDict()
        self.orders_by_id = {}  # A dictionary to access all orders by id
        self.bid_qty_by_price = SortedDict(OrderBook._return_neg)
        self.ask_qty_by_price = SortedDict()

    def on_new_message(self, message : Message):
        """
        Method to handle different types of messages and modify the order book accordingly.
        returns a tuple consisting of the price level, updated order's queue location, the sum of the quantities 
        at this price level up to the updated order.

        returns : (asset, price, sum_qty, q_loc, flag)
        """
        self.current_ts=message.ts
        res=None
        if message.type == 'A':
            res = self.add_order(message)
        elif message.type == 'D':
            res = self.delete_order(message)
        elif message.type == 'E':
            res = self.execute_order(message)
        else:#it is a message about an event in bist
            #self._reset_()
            if message.flag in ['P_GUNSONU','P_ACS_EMR_TP_PY_EIY']:
                self._reset_levels_()
        """
        array(['P_GUNSONU', 'P_ACS_EMR_TP_PY_EIY', 'P_ESLESTIRME', 'NONE',
            'P_MARJ_YAYIN', 'P_ARA', 'P_SUREKLI_ISLEM', 'P_MARJ_YAYIN_KAPANIS',
            'P_KAPANIS_FIY_ISLEM', 'P_GUNSONU_ISLEMLERI', 'P_KAPANIS_EMIR_TPL',
            'P_DK_TEKFIY_EMIR_TPL'], dtype=object)
        """
        self.prev_ts=message.ts
        """
        ##############################
        self.book['ts'].append(message.ts)
        self.book['askpx'].append(self.get_best_ask()[0])
        self.book['askqty'].append(self.get_best_ask()[1])
        self.book['askpx2'].append(self.get_best_ask(1)[0])
        self.book['askqty2'].append(self.get_best_ask(1)[1])
        self.book['askpx3'].append(self.get_best_ask(2)[0])
        self.book['askqty3'].append(self.get_best_ask(2)[1])

        self.book['bidpx'].append(self.get_best_bid()[0])
        self.book['bidqty'].append(self.get_best_bid()[1])
        self.book['bidpx2'].append(self.get_best_bid(1)[0])
        self.book['bidqty2'].append(self.get_best_bid(1)[1])
        self.book['bidpx3'].append(self.get_best_bid(2)[0])
        self.book['bidqty3'].append(self.get_best_bid(2)[1])

        ##############################
        """


        return res

    
    def add_order(self, message):
        """
        Adds an order to the book based on its type.
        returns a tuple consisting of the price level, total number of orders in the price level, total quantity of orders up to the deleted order,
        updated order's queue location, the flag representing the type of the message(D,E,R) A in this case.
        returns : (price, sum_qty, q_loc, 'D') 
        """
        id_by_price = self.bid_ids_by_price if message.side == 'B' else self.ask_ids_by_price
        order_dict = self.orders_by_id
        qty_by_price= self.bid_qty_by_price if message.side == 'B' else self.ask_qty_by_price
        
        if message.price not in list(id_by_price.keys()):
            id_by_price[message.price] = deque()
            qty_by_price[message.price] = 0

        level_q=id_by_price[message.price]
        level_q.append(message.id)
        order_dict[message.id] = message
        qty_by_price[message.price] += message.qty
        return self.asset, message.price,qty_by_price[message.price],len(level_q)-1,'A',message.side
        
    def delete_order(self, message):
        """
        Deletes an order from the book.
        returns a tuple consisting of the price level, total number of orders in the price level, total quantity of orders up to the deleted order,
        updated order's queue location, the flag representing the type of the message(D,E,R) D in this case.
        
        returns : (price, sum_qty, q_loc, 'D') 
        """
        id_by_price = self.bid_ids_by_price if message.side == 'B' else self.ask_ids_by_price
        order_dict = self.orders_by_id
        qty_by_price = self.bid_qty_by_price if message.side == 'B' else self.ask_qty_by_price
        
        order = order_dict.get(message.id)
        if order is None:
            logging.warning(f"D: Order {message.id} does not exist.")
            return
        #remove the order from the level queue
        level_q=id_by_price[order.price]
        index=0
        q_loc=0
        sum_qty_tmp=0
        sum_qty=0
        tempQ=deque()
        while len(level_q)>0:
            next_id=level_q.popleft()
            if next_id==message.id:
                q_loc=index
                sum_qty=sum_qty_tmp
            else:
                sum_qty_tmp+=order_dict[next_id].qty
                tempQ.append(next_id)
            index+=1
        id_by_price[order.price]=tempQ
        
        #delete the order from the order by id in all cases
        qty_by_price[order.price] -= order.qty
        #if this was the last order at this price
        if qty_by_price[order.price]==0:
            del qty_by_price[order.price]
            del id_by_price[order.price]
            
        del order_dict[message.id]
        return self.asset, message.price,sum_qty,q_loc,'D',message.side

    def execute_order(self, message):
        id_by_price = self.bid_ids_by_price if message.side == 'B' else self.ask_ids_by_price
        order_dict = self.orders_by_id
        qty_by_price = self.bid_qty_by_price if message.side == 'B' else self.ask_qty_by_price
        
        order=order_dict.get(message.id)
        if order is None:
            #this is possible if we run a taker strategy and the order is already executed
            logging.warning(f"E: Order {message.id} does not exist.")
            return
        qty_by_price[order.price] -= message.qty
        level_q=id_by_price[order.price]
        order.qty-=message.qty
        if order.qty==0:
            level_q.remove(message.id)
            del order_dict[message.id]
        #if this was the last order at this price
        if qty_by_price[order.price]==0:
            del qty_by_price[order.price]
            del id_by_price[order.price]
        
        return self.asset, message.price,0,0,'E',message.side

    def _get_qty_by_price(self, side, price):
        """
        Returns the total quantity at a given price on a given side. for checling if the id byprice works correctly
        """
        id_by_price = self.bid_ids_by_price if side == 'B' else self.ask_ids_by_price  
        q=id_by_price.get(price,[])
        qty=0
        for id_ in q:
            qty+=self.orders_by_id[id_].qty
        return qty

    def get_best_bid(self, level=0):
        """
        Returns the best bid price and quantity at a given level.
        """
        if len(self.bid_qty_by_price) > level:
            price = list(self.bid_qty_by_price.keys())[level]
            qty = self.bid_qty_by_price[price]
            return price, qty
        else:
            return None, None
    
    def get_best_ask(self, level=0):
        """
        Returns the best ask price and quantity at a given level.
        """
        if len(self.ask_qty_by_price) > level:
            price = list(self.ask_qty_by_price.keys())[level]
            qty = self.ask_qty_by_price[price]
            return price, qty
        else:
            return None, None
