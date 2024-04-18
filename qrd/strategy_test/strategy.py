#from qrd.strategy_test.strategy import Strategy
from sortedcontainers import SortedDict
import pandas as pd 
import qrd.data.utils as utils
import numpy as np
import matplotlib.pyplot as plt
class Order:
    def __init__(self,type,order_id,price,quantity,side,flag,enter_ts):
        self.type=type
        self.order_id=order_id
        self.price=price
        self.quantity=quantity
        self.side=side
        self.flag=flag
        self.qty_in_front=0
        self.enter_ts=enter_ts
        self.accepted_ts=None
        


class Strategy:
    def __init__(self,sent_latency=pd.Timedelta(0,unit='ns'), commission=0.00005):
        self.bids : SortedDict[float,list[Order]]=SortedDict()#bst for the bids with keys price and values queue of orders
        self.offers : SortedDict[float,list[Order]]=SortedDict()
        self.inventory=0
        self.cash=0
        self.ouch=[]
        self.order_ts={}#we are keeping a dict to save the order ts for the orders key:id, value:ts
        self.current_row=None
        self.id_counter=0
        #self.qty_by_px={}
        self.bidqty_by_px={}
        self.askqty_by_px={}
        self.sent_latency=sent_latency
        self.orders_on_way=[]
        self.commission=commission
        self.prev_row=None
        self.prevMold=None
    
    def get_inventory(self):
        return self.inventory

    def get_bid_qty(self,px):
        return self.bidqty_by_px.get(px,0)
    
    def get_ask_qty(self,px):
        return self.askqty_by_px.get(px,0)
    
    def get_price_step(self,price):
        if price<20*1000:
            fiyat_adimi = 0.01*1000
        elif price<50*1000:
            fiyat_adimi = 0.02*1000
        elif price<100*1000:
            fiyat_adimi = 0.05*1000
        elif price<250*1000:
            fiyat_adimi = 0.10*1000
        elif price<500*1000:
            fiyat_adimi = 0.25*1000
        elif price<1000*1000:
            fiyat_adimi = 0.50*1000         
        elif price<2500*1000:
            fiyat_adimi = 1*1000             
        else:
            fiyat_adimi = 2.5*1000
        return fiyat_adimi

    

    def add_data(self,data : pd.DataFrame, messages : pd.DataFrame=None):
        if type(data.index)!=pd.core.indexes.datetimes.DatetimeIndex:
            raise ValueError('index must be datetime')
        self.row_data=data
        if messages is None:
            if 'messages' not in data.columns:
            #maybe more checks later
                raise ValueError('messages column not found')
            msgs=utils.extract_messages(data['messages'])
        else:
            msgs=messages
        self.data=msgs.join(data,how='outer')
        if "messages" in self.data.columns:
            self.data=self.data.drop(columns=['messages'])
        self.data['ts']=self.data.index
        self.data['next_ts']=self.data['ts'].shift(-1).fillna(method='ffill')

    #this will be overrided by the user
    def on_mold_update(self,row):
        """
        This function will be called when the mold is updated.
        """
        raise NotImplementedError('on_mold_update must be implemented')

    #this will be overrided by the user
    def on_trade(self,order,event):
        """
        This function will be called when an order is executed or accepted.
        """
        pass
    
    def check_buy_execution(self,row):            
        qty_to_be_executed=row.qty
        while len(self.bids)>0 and row.px <= self.bids.peekitem(-1)[0] and qty_to_be_executed>0 and len(self.bids.peekitem(-1)[1])>0:
            best_bid_px=self.bids.peekitem(-1)[0]
            order=self.bids[best_bid_px][0]
            exec_qty=min(qty_to_be_executed,order.quantity+order.qty_in_front)
            order.qty_in_front-=exec_qty
            qty_to_be_executed-=exec_qty
            if order.qty_in_front<=0:
                exec_qty=min(-order.qty_in_front,order.quantity)
                order.quantity-=exec_qty
                qty_to_be_executed-=exec_qty
                self.cash-=exec_qty*order.price*(1+self.commission)
                self.inventory+=exec_qty
                order.qty_in_front=0
                if order.quantity==0:
                    self.bids[best_bid_px].pop(0)
                ouch_msg={
                    'event':'execution',
                    'ts':row.ts,
                    'order_id':order.order_id,
                    'price':order.price,
                    'quantity':exec_qty,
                    'type':'E',
                    'side':order.side,
                    'inventory':self.inventory,
                    'cash':self.cash,
                }
                self.ouch.append(ouch_msg)
                #calling on_trade
                self.on_trade(order,'execution')
                
            if len(self.bids[best_bid_px])==0:
                self.bids.pop(best_bid_px)

    def check_sell_execution(self,row):
        qty_to_be_executed=row.qty
        while len(self.offers)>0 and row.px >= self.offers.peekitem(0)[0] and qty_to_be_executed>0 and len(self.offers.peekitem(0)[1])>0:
            best_offer_px=self.offers.peekitem(0)[0]
            order=self.offers[best_offer_px][0]
            exec_qty=min(qty_to_be_executed,order.quantity+order.qty_in_front)
            order.qty_in_front-=exec_qty
            qty_to_be_executed-=exec_qty
            if order.qty_in_front<=0:
                exec_qty=min(-order.qty_in_front,order.quantity)
                order.quantity-=exec_qty
                qty_to_be_executed-=exec_qty
                self.cash+=exec_qty*order.price*(1-self.commission)
                self.inventory-=exec_qty
                order.qty_in_front=0
                if order.quantity==0:
                    self.offers[best_offer_px].pop(0)                        
                ouch_msg={
                    'event':'execution',
                    'ts':row.ts,
                    'order_id':order.order_id,
                    'price':order.price,
                    'quantity':exec_qty,
                    'side':order.side,
                    'type':'E',
                    'cash' : self.cash,
                    'inventory':self.inventory
                }    
                self.ouch.append(ouch_msg)
                #calling on_trade
                self.on_trade(order,'execution')
        
            if len(self.offers[best_offer_px])==0:
                self.offers.pop(best_offer_px)   


    def adjust_queue_locs(self,row):
        if row.Type=='A':
            #self.qty_by_px[row.px]=self.qty_by_px.get(row.px,0)+row.qty
            if row.Direction=='B':
                self.bidqty_by_px[row.px]=self.bidqty_by_px.get(row.px,0)+row.qty
            else:
                self.askqty_by_px[row.px]=self.askqty_by_px.get(row.px,0)+row.qty
            #if it is actually a replacement on same price with a lower qty
            if row.flag=='SIZE_REDUCTION':
                if row.Direction=='B':
                    if row.px in self.bids:
                        for order in self.bids[row.px]:
                            if self.order_ts[row.id]>order.accepted_ts:
                                order.qty_in_front+=row.qty-self.prev_row.qty
                else:
                    if row.px in self.offers:
                        for order in self.offers[row.px]:
                            if self.order_ts[row.id]>order.accepted_ts:
                                order.qty_in_front+=row.qty-self.prev_row.qty
            else:
                self.order_ts[row.id]=row.ts
                #checking the execution possibility
                if row.Direction=='B':
                    self.check_sell_execution(row)      
                else:
                    self.check_buy_execution(row)
        elif row.Type=='E':    
            if row.Direction=='B':
                self.bidqty_by_px[row.px]-=row.qty
                self.check_buy_execution(row)
            else:
                self.askqty_by_px[row.px]-=row.qty
                self.check_sell_execution(row)

        elif row.Type=='D':
            #self.qty_by_px[row.px]-=row.qty
            if row.flag!='SIZE_REDUCTION':
                if row.Direction=='B':
                    self.bidqty_by_px[row.px]-=row.qty  
                    if row.px in self.bids:
                        for order in self.bids[row.px]:
                            if self.order_ts[row.id]<=order.accepted_ts:
                                order.qty_in_front-=row.qty
                elif row.Direction=='S':
                    self.askqty_by_px[row.px]-=row.qty
                    if row.px in self.offers:
                        for order in self.offers[row.px]:
                            if self.order_ts[row.id]<=order.accepted_ts:
                                order.qty_in_front-=row.qty
                del self.order_ts[row.id]

    
    def add_order(self,type,side,price,quantity):
        order=Order(type,self.id_counter,price,quantity,side,'N',self.current_row.ts)
        self.id_counter+=1
        self.orders_on_way.append(order)
        ouch_msg={
            'event':'enter_order',
            'ts':self.current_row.ts,
            'order_id':order.order_id,
            'price':price,
            'quantity':quantity,
            'side':side,
            'type':type,
            'cash':self.cash,
            'inventory':self.inventory
        }
        self.ouch.append(ouch_msg)
        #we will log the event as enter order
    
    def delete_order(self,order_id,price,qty,side):
        order=Order('D',order_id,price,qty,side,'N',self.current_row.ts)
        self.orders_on_way.append(order)
        ouch_msg={
            'event':'enter_order',
            'ts':self.current_row.ts,
            'order_id':order.order_id,
            'price':price,
            'quantity':order.quantity,
            'side':order.side,
            'type':order.type,
            'cash':self.cash,
            'inventory':self.inventory
        }
        self.ouch.append(ouch_msg)

    def delete_all_orders(self):
        for order in self.orders_on_way:
            if order.type!='D':
                self.delete_order(order.order_id,order.price,order.quantity,order.side)
        for px in self.bids:
            for order in self.bids[px]:
                self.delete_order(order.order_id,order.price,order.quantity,order.side)
        for px in self.offers:
            for order in self.offers[px]:
                self.delete_order(order.order_id,order.price,order.quantity,order.side)
    
    def run(self,data=None):
        if data is not None:
            self.add_data(data)
        if self.data is None:
            raise ValueError('data is not set, use add_data method to set the data first')
        prev_ts=None
        for row in self.data.itertuples():
            #checking if there is at least 8 hour difference between the rows or açılış mesajı geldi mi, if so we will reset the strategy
            if (prev_ts!=None and row.ts-prev_ts>pd.Timedelta(8,unit='h')) or row.flag=='P_ESLESTIRME':
                self.bids.clear()
                self.offers.clear()
                #self.qty_by_px.clear()
                self.bidqty_by_px.clear()
                self.askqty_by_px.clear()
                self.orders_on_way.clear()
                self.order_ts.clear()

            self.adjust_queue_locs(row)   
            #last row of the mold
            if row.ts!=row.next_ts:
                self.current_row=row
                #checking if our orders are came to exchange or what
                while len(self.orders_on_way)>0 and self.orders_on_way[0].enter_ts+self.sent_latency<=row.ts:
                    order=self.orders_on_way.pop(0)
                    order.accepted_ts=row.ts
                    if order.side=='B':
                        order.qty_in_front=self.bidqty_by_px.get(order.price,0)
                    else:
                        order.qty_in_front=self.askqty_by_px.get(order.price,0)
                    #logging the enter accepted
                    ouch_msg={
                        'event':'accept_order',
                        'ts':row.ts,
                        'order_id':order.order_id,
                        'price':order.price,
                        'quantity':order.quantity,
                        'side':order.side,
                        'type':order.type,
                        'cash':self.cash,
                        'inventory':self.inventory
                    }
                    self.ouch.append(ouch_msg)
                    #calling on_trade
                    self.on_trade(order,'accept_order')
                    
                    #handling deletions first
                    if order.type=='D':
                        if order.side=='B':
                            found=False
                            if order.price in self.bids:
                                for i in range(len(self.bids[order.price])):
                                    if self.bids[order.price][i].order_id==order.order_id:
                                        self.bids[order.price].pop(i)
                                        found=True
                                        if self.bids[order.price]==[]:
                                            self.bids.pop(order.price)
                                        break
                                
                            if not found:
                                pass
                                #print('order to be deleted not found with id:',order.order_id)
                        else:
                            found=False
                            if order.price in self.offers:
                                for i in range(len(self.offers[order.price])):
                                    if self.offers[order.price][i].order_id==order.order_id:
                                        self.offers[order.price].pop(i)
                                        found=True
                                        if self.offers[order.price]==[]:
                                            self.offers.pop(order.price)
                                        break
                            if not found:
                                #raise ValueError('order to be deleted not found with id:',order.order_id)
                                #print('order to be deleted not found with id:',order.order_id)
                                pass
                    elif order.side=='B' and order.type=='A':
                        #first, checking the execution possibility
                        if row.askpx<=order.price:
                            exec_qty=min(order.quantity,row.askqty)
                            order.quantity-=exec_qty
                            self.cash-=exec_qty*order.price*(1+self.commission)
                            self.inventory+=exec_qty
                            if order.quantity>0:
                                if order.price in self.bids:
                                    self.bids[order.price].append(order)
                                else:
                                    self.bids[order.price]=[order]
                            #logging the execution
                            ouch_msg={
                                'event':'execution',
                                'ts':row.ts,
                                'order_id':order.order_id,
                                'price':order.price,
                                'quantity':exec_qty,
                                'side':'B',
                                'type':'E',
                                'cash':self.cash,
                                'inventory':self.inventory
                            }
                            self.ouch.append(ouch_msg)
                            #calling on_trade
                            self.on_trade(order,'execution')
                        elif order.price in self.bids:
                            self.bids[order.price].append(order)
                        else:
                            self.bids[order.price]=[order]
                    elif order.side=='S' and order.type=='A':
                        #first, checking the execution possibility
                        if row.bidpx>=order.price:
                            exec_qty=min(order.quantity,row.bidqty)
                            order.quantity-=exec_qty
                            self.cash+=exec_qty*order.price*(1-self.commission)
                            self.inventory-=exec_qty
                            if order.quantity>0:
                                if order.price in self.offers:
                                    self.offers[order.price].append(order)
                                else:
                                    self.offers[order.price]=[order]
                            #logging the execution
                            ouch_msg={
                                'event':'execution',
                                'ts':row.ts,
                                'order_id':order.order_id,
                                'price':order.price,
                                'quantity':exec_qty,
                                'side':'S',
                                'type':'E',
                                'cash':self.cash,
                                'inventory':self.inventory
                            }
                            self.ouch.append(ouch_msg)
                            #calling on_trade
                            self.on_trade(order,'execution')
                        elif order.price in self.offers:
                            self.offers[order.price].append(order)
                        else:
                            self.offers[order.price]=[order]
                
                self.on_mold_update(row)
                #sıraya dikkat!!!
                self.prevMold=row

            prev_ts=row.ts
            self.prev_row=row
        if len(self.ouch)==0:
            raise ValueError('no ouch message is generated, check the data or strategy')
        self.ouch=pd.DataFrame(self.ouch).set_index('ts',drop=True)
        return self.ouch
    
    def get_results(self, plot=False,additional_columns=[]):
        """
        calculate inventory value and PnL, some additional columns can be added to the results to check the strategy.
        There will be other metrics to be added later.
        """
        data=self.row_data
        ouch=self.ouch
        default_cols=['askpx', 'bidpx', 'askqty','bidqty','teo', 'messages']
        combined = ouch.join(data[list(set(default_cols+additional_columns))], how='outer')
        
        # Forward-fill missing values for specified columns
        combined['inventory'] = combined['inventory'].fillna(method='ffill')
        combined['cash'] = combined['cash'].fillna(method='ffill')
        combined['askpx'] = combined['askpx'].fillna(method='ffill')
        combined['bidpx'] = combined['bidpx'].fillna(method='ffill')
        combined['teo'] = combined['teo'].fillna(method='ffill')
        combined['askqty'] = combined['askqty'].fillna(method='ffill')
        combined['bidqty'] = combined['bidqty'].fillna(method='ffill')
        combined['messages'] = combined['messages'].fillna(method='ffill')
        # Calculate inventory value and PnL
        combined['inv_value'] = combined['inventory'] * np.where(combined['inventory'] > 0, combined['bidpx'], combined['askpx'])
        combined['pnl'] = combined['cash'] + combined['inv_value']
        combined['pnl_diff'] = combined['pnl'].diff().fillna(0)
        
        # If plotting is requested, plot the PnL
        if plot:
            combined.reset_index(drop=True)['pnl'].plot(title='PnL Over Time')
            plt.xlabel('Time')
            plt.ylabel('PnL')
            plt.show()
        
        # Return the combined DataFrame with calculations
        return combined
        

#from qrd.strategy_test.strategy import Strategy
class TeoStrategyExample(Strategy):

    def __init__(self,inventory_limit=10,default_lots=1,sent_latency=pd.Timedelta(0,unit='ns'),ticksize=250,
                 closing_time=pd.Timestamp('17:58:00').time(),opening_time=pd.Timestamp('10:00:00').time(),time_diff=pd.Timedelta(20,unit='s'),
                 volume_limit=348709.0):
        super().__init__(sent_latency)
        self.inventory_limit=inventory_limit
        self.default_lots=default_lots
        self.closing_time=closing_time
        self.opening_time=opening_time 
        self.time_diff=time_diff
        self.ticksize=ticksize
        self.volume_limit=volume_limit  
        self.last_buy_signal_ts=None
        self.last_sell_signal_ts=None

    def on_mold_update(self, row):
        current_time=self.current_row.ts.time()

        if current_time>self.opening_time and current_time<self.closing_time:    
            buy_time_cond= self.last_buy_signal_ts is None or row.ts-self.last_buy_signal_ts>self.time_diff
            sell_time_cond= self.last_sell_signal_ts is None or row.ts-self.last_sell_signal_ts>self.time_diff
            upflip=self.prev_row.askpx<row.askpx
            downflip=self.prev_row.bidpx>row.bidpx
            if row.teo<row.bidpx and not upflip and not downflip and self.inventory>-self.inventory_limit:
                #if there is no waiting order on the way and there is no offer at the price
                if row.bidpx not in [order.price for order in self.orders_on_way if order.side=='S' and order.type=='A'] and row.bidpx not in self.offers:
                    if self.inventory>0:
                        #first deleting all orders
                        for px in self.bids:
                            for order in self.bids[px]:
                                self.delete_order(order.order_id,order.price,order.quantity,order.side) 

                        for px in self.offers:
                            if px!=row.bidpx:
                                for order in self.offers[px]:
                                    self.delete_order(order.order_id,order.price,order.quantity,order.side)

                        self.add_order('A','S',row.bidpx,self.inventory),
                        self.last_sell_signal_ts=row.ts
                        
                    elif (row.volume_sum>self.volume_limit and sell_time_cond):
                        #first deleting all orders
                        for px in self.bids:
                            for order in self.bids[px]:
                                self.delete_order(order.order_id,order.price,order.quantity,order.side) 

                        for px in self.offers:
                            if px!=row.bidpx:
                                for order in self.offers[px]:
                                    self.delete_order(order.order_id,order.price,order.quantity,order.side)
                            
                        self.add_order('A','S',row.bidpx,self.default_lots)
                        self.last_sell_signal_ts=row.ts

            if row.teo>row.askpx and not upflip and not downflip and self.inventory<self.inventory_limit:
                if row.askpx not in [order.price for order in self.orders_on_way if order.side=='B' and order.type=='A'] and row.askpx not in self.bids:
                    #first deleting all orders
                    if self.inventory<0:
                        for px in self.bids:
                            if px!=row.askpx:
                                for order in self.bids[px]:
                                    self.delete_order(order.order_id,order.price,order.quantity,order.side)
                                    
                        for px in self.offers:
                            for order in self.offers[px]:
                                self.delete_order(order.order_id,order.price,order.quantity,order.side) 
                        self.add_order('A','B',row.askpx,-self.inventory)
                        self.last_buy_signal_ts=row.ts
                        
                    elif row.volume_sum>self.volume_limit and buy_time_cond :
                        for px in self.bids:
                            if px!=row.askpx:
                                for order in self.bids[px]:
                                    self.delete_order(order.order_id,order.price,order.quantity,order.side)
                        for px in self.offers:
                            for order in self.offers[px]:
                                self.delete_order(order.order_id,order.price,order.quantity,order.side) 
    
                        self.add_order('A','B',row.askpx,self.default_lots)
                        self.last_buy_signal_ts=row.ts
                    


        elif current_time>=self.closing_time:
            if len(self.orders_on_way)==0:
                self.delete_all_orders()
                #closing all positions
                if self.inventory>0:
                    self.add_order('A','S',row.bidpx,self.inventory)
                elif self.inventory<0:
                    self.add_order('A','B',row.askpx,-self.inventory)

        


