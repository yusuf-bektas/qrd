#from qrd.strategy_test.strategy import Strategy
from sortedcontainers import SortedDict
import pandas as pd 
import qrd.data.utils as utils

class Order:
    def __init__(self,asset,type,order_id,price,quantity,side,flag,enter_ts):
        self.asset=asset
        self.type=type
        self.order_id=order_id
        self.price=price
        self.quantity=quantity
        self.side=side
        self.flag=flag
        self.qty_in_front=0
        self.enter_ts=enter_ts
        self.accepted_ts=None
        


class MultiAssetStrategy:
    
    def __init__(self,sent_latency=pd.Timedelta(500,unit='ns'), commission=0.00005):
        self.bids : dict[str,SortedDict[float,list[Order]]] = {}
        self.offers : dict[str,SortedDict[float,list[Order]]] = {}
        self.inventory : dict[str,int] = {}
        self.cash=0
        self.ouch=[]
        self.order_ts={} #we are keeping a dict to save the order ts for the orders key:id, value:ts
        self.current_row=None
        self.id_counter=0
        #self.qty_by_px={}
        self.bidqty_by_px : dict[str,dict[float,int]] = {}
        self.askqty_by_px : dict[str,dict[float,int]] = {}
        self.sent_latency=sent_latency
        self.orders_on_way : dict[str,list[Order]] = {}
        self.commission=commission
        self.prev_row=None

    #this will be overrided by the user
    def on_mold_update(self,row):
        raise NotImplementedError('on_mold_update must be implemented')
    
    def check_buy_execution(self,row):            
        qty_to_be_executed=row.qty
        while len(self.bids[row.asset])>0 and row.px <= self.bids[row.asset].peekitem(-1)[0] and qty_to_be_executed>0 and len(self.bids[row.asset].peekitem(-1)[1])>0:
            best_bid_px=self.bids[row.asset].peekitem(-1)[0]
            order=self.bids[row.asset][best_bid_px][0]
            exec_qty=min(qty_to_be_executed,order.quantity+order.qty_in_front)
            order.qty_in_front-=exec_qty
            qty_to_be_executed-=exec_qty
            if order.qty_in_front<=0:
                exec_qty=min(-order.qty_in_front,order.quantity)
                order.quantity-=exec_qty
                qty_to_be_executed-=exec_qty
                self.cash-=exec_qty*order.price*(1+self.commission)
                self.inventory[row.asset]+=exec_qty
                order.qty_in_front=0
                if order.quantity==0:
                    self.bids[row.asset][best_bid_px].pop(0)
                ouch_msg={
                    'event':'execution',
                    'asset':row.asset,
                    'ts':row.ts,
                    'order_id':order.order_id,
                    'price':order.price,
                    'quantity':exec_qty,
                    'type':'E',
                    'side':order.side,
                    'inventory':self.inventory[row.asset],
                    'cash':self.cash,
                }
                self.ouch.append(ouch_msg)
                
            if len(self.bids[row.asset][best_bid_px])==0:
                self.bids[row.asset].pop(best_bid_px)

    def check_sell_execution(self,row):
        qty_to_be_executed=row.qty
        while len(self.offers[row.asset])>0 and row.px >= self.offers[row.asset].peekitem(0)[0] and qty_to_be_executed>0 and len(self.offers[row.asset].peekitem(0)[1])>0:
            best_offer_px=self.offers[row.asset].peekitem(0)[0]
            order=self.offers[row.asset][best_offer_px][0]
            exec_qty=min(qty_to_be_executed,order.quantity+order.qty_in_front)
            order.qty_in_front-=exec_qty
            qty_to_be_executed-=exec_qty
            if order.qty_in_front<=0:
                exec_qty=min(-order.qty_in_front,order.quantity)
                order.quantity-=exec_qty
                qty_to_be_executed-=exec_qty
                self.cash+=exec_qty*order.price*(1-self.commission)
                self.inventory[row.asset]-=exec_qty
                order.qty_in_front=0
                if order.quantity==0:
                    self.offers[row.asset][best_offer_px].pop(0)                        
                ouch_msg={
                    'event':'execution',
                    'asset':row.asset,
                    'ts':row.ts,
                    'order_id':order.order_id,
                    'price':order.price,
                    'quantity':exec_qty,
                    'side':order.side,
                    'type':'E',
                    'cash' : self.cash,
                    'inventory':self.inventory[row.asset]
                }    
                self.ouch.append(ouch_msg)
        
            if len(self.offers[row.asset][best_offer_px])==0:
                self.offers[row.asset].pop(best_offer_px)   


    def adjust_queue_locs(self,row):
        if row.Type=='A':
            #self.qty_by_px[row.px]=self.qty_by_px.get(row.px,0)+row.qty
            if row.Direction=='B':
                self.bidqty_by_px[row.asset][row.px]=self.bidqty_by_px[row.asset].get(row.px,0)+row.qty
            else:
                self.askqty_by_px[row.asset][row.px]=self.askqty_by_px[row.asset].get(row.px,0)+row.qty
            #if it is actually a replacement on same price with a lower qty
            if row.flag=='SIZE_REDUCTION':
                if row.Direction=='B':
                    if row.px in self.bids[row.asset]:
                        for order in self.bids[row.asset][row.px]:
                            if self.order_ts[row.asset][row.id]>order.accepted_ts:
                                order.qty_in_front+=row.qty-self.prev_row.qty
                else:
                    if row.px in self.offers[row.asset]:
                        for order in self.offers[row.asset][row.px]:
                            if self.order_ts[row.asset][row.id]>order.accepted_ts:
                                order.qty_in_front+=row.qty-self.prev_row.qty
            else:
                self.order_ts[row.asset][row.id]=row.ts
                #checking the execution possibility
                if row.Direction=='B':
                    self.check_sell_execution(row)      
                else:
                    self.check_buy_execution(row)

        elif row.Type=='E':    
            if row.Direction=='B':
                self.bidqty_by_px[row.asset][row.px]-=row.qty
                self.check_buy_execution(row)
            else:
                self.askqty_by_px[row.asset][row.px]-=row.qty
                self.check_sell_execution(row)

        elif row.Type=='D':
            #self.qty_by_px[row.px]-=row.qty
            if row.flag!='SIZE_REDUCTION':
                if row.Direction=='B':
                    self.bidqty_by_px[row.asset][row.px]-=row.qty  
                    if row.px in self.bids[row.asset]:
                        for order in self.bids[row.asset][row.px]:
                            if self.order_ts[row.asset][row.id]<=order.accepted_ts:
                                order.qty_in_front-=row.qty
                
                elif row.Direction=='S':
                    self.askqty_by_px[row.asset][row.px]-=row.qty
                    if row.px in self.offers:
                        for order in self.offers[row.asset][row.px]:
                            if self.order_ts[row.asset][row.id]<=order.accepted_ts:
                                order.qty_in_front-=row.qty
                del self.order_ts[row.asset][row.id]
    
    def add_order(self,asset,type,side,price,quantity):
        order=Order(asset,type,self.id_counter,price,quantity,side,'N',self.current_row.ts)
        self.id_counter+=1
        self.orders_on_way[row.asset].append(order)
        ouch_msg={
            'event':'enter_order',
            'asset':asset,
            'ts':self.current_row.ts,
            'order_id':order.order_id,
            'price':price,
            'quantity':quantity,
            'side':side,
            'type':type,
            'cash':self.cash,
            'inventory':self.inventory[asset]
        }
        self.ouch.append(ouch_msg)
        #we will log the event as enter order
    
    def delete_order(self,asset,order_id,price,qty,side):
        order=Order(asset,'D',order_id,price,qty,side,'N',self.current_row.ts)
        self.orders_on_way[row.asset].append(order)
        ouch_msg={
            'event':'enter_order',
            'asset':asset,
            'ts':self.current_row.ts,
            'order_id':order.order_id,
            'price':price,
            'quantity':order.quantity,
            'side':order.side,
            'type':order.type,
            'cash':self.cash,
            'inventory':self.inventory[asset]
        }
        self.ouch.append(ouch_msg)

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

    
    
    def run(self,data:pd.DataFrame, assets : list) -> pd.DataFrame:
        self.data=data
        self.data['ts']=self.data.index
        self.data['next_ts']=self.data['ts'].shift(-1).fillna(method='ffill')
        for asset in assets:
            self.bids[asset]=SortedDict()
            self.offers[asset]=SortedDict()
            self.inventory[asset]=0
            self.bidqty_by_px[asset]={}
            self.askqty_by_px[asset]={}
            self.orders_on_way[asset]=[]
            self.order_ts[asset]={}

        if self.data is None:
            raise ValueError('data is not set, use add_data method to set the data first')
        
        prev_ts=None
        for row in self.data.itertuples():
            #checking if there is at least 8 hour difference between the rows or açılış mesajı geldi mi, if so we will reset the strategy
            if (prev_ts!=None and row.ts-prev_ts>pd.Timedelta(8,unit='h')) or row.flag=='P_ESLESTIRME':
                self.bids[row.asset].clear()
                self.offers[row.asset].clear()
                #self.qty_by_px.clear()
                self.bidqty_by_px[row.asset].clear()
                self.askqty_by_px[row.asset].clear()
                self.orders_on_way[row.asset].clear()
                self.order_ts[row.asset].clear()

            self.adjust_queue_locs(row)   
            #last row of the mold
            if row.ts!=row.next_ts:
                self.current_row=row
                #checking if our orders are came to exchange or what
                while len(self.orders_on_way[row.asset])>0 and self.orders_on_way[row.asset][0].enter_ts+self.sent_latency<=row.ts:
                    order=self.orders_on_way[row.asset].pop(0)
                    if order.asset==row.asset:
                        order.accepted_ts=row.ts
                        if order.side=='B':
                            order.qty_in_front=self.bidqty_by_px[row.asset].get(order.price,0)
                        else:
                            order.qty_in_front=self.askqty_by_px[row.asset].get(order.price,0)
                        #logging the enter accepted
                        ouch_msg={
                            'event':'accept_order',
                            'asset':order.asset,
                            'ts':row.ts,
                            'order_id':order.order_id,
                            'price':order.price,
                            'quantity':order.quantity,
                            'side':order.side,
                            'type':order.type,
                            'cash':self.cash,
                            'inventory':self.inventory[row.asset]
                        }
                        self.ouch.append(ouch_msg)
                        
                        #handling deletions first
                        if order.type=='D':
                            if order.side=='B':
                                found=False
                                if order.price in self.bids[row.asset]:
                                    for i in range(len(self.bids[row.asset][order.price])):
                                        if self.bids[row.asset][order.price][i].order_id==order.order_id:
                                            self.bids[row.asset][order.price].pop(i)
                                            found=True
                                            if self.bids[row.asset][order.price]==[]:
                                                self.bids[row.asset].pop(order.price)
                                            break
                                    
                                if not found:
                                    pass
                                    #print('order to be deleted not found with id:',order.order_id)
                            else:
                                found=False
                                if order.price in self.offers[row.asset]:
                                    for i in range(len(self.offers[row.asset][order.price])):
                                        if self.offers[row.asset][order.price][i].order_id==order.order_id:
                                            self.offers[row.asset][order.price].pop(i)
                                            found=True
                                            if self.offers[row.asset][order.price]==[]:
                                                self.offers[row.asset].pop(order.price)
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
                                self.inventory[row.asset]+=exec_qty
                                if order.quantity>0:
                                    if order.price in self.bids[row.asset]:
                                        self.bids[row.asset][order.price].append(order)
                                    else:
                                        self.bids[row.asset][order.price]=[order]
                                #logging the execution
                                ouch_msg={
                                    'event':'execution',
                                    'asset':row.asset,
                                    'ts':row.ts,
                                    'order_id':order.order_id,
                                    'price':order.price,
                                    'quantity':exec_qty,
                                    'side':'B',
                                    'type':'E',
                                    'cash':self.cash,
                                    'inventory':self.inventory[row.asset]
                                }
                                self.ouch.append(ouch_msg)
                            elif order.price in self.bids[row.asset]:
                                self.bids[row.asset][order.price].append(order)
                            else:
                                self.bids[row.asset][order.price]=[order]

                        elif order.side=='S' and order.type=='A':
                            #first, checking the execution possibility
                            if row.bidpx>=order.price:
                                exec_qty=min(order.quantity,row.bidqty)
                                order.quantity-=exec_qty
                                self.cash+=exec_qty*order.price*(1-self.commission)
                                self.inventory[row.asset]-=exec_qty
                                if order.quantity>0:
                                    if order.price in self.offers:
                                        self.offers[row.asset][order.price].append(order)
                                    else:
                                        self.offers[row.asset][order.price]=[order]
                                #logging the execution
                                ouch_msg={
                                    'event':'execution',
                                    'asset':row.asset,
                                    'ts':row.ts,
                                    'order_id':order.order_id,
                                    'price':order.price,
                                    'quantity':exec_qty,
                                    'side':'S',
                                    'type':'E',
                                    'cash':self.cash,
                                    'inventory':self.inventory[row.asset]
                                }
                                self.ouch.append(ouch_msg)
                            elif order.price in self.offers:
                                self.offers[order.price].append(order)
                            else:
                                self.offers[order.price]=[order]
                
                self.on_mold_update(row)

            prev_ts=row.ts
            self.prev_row=row
        self.ouch=pd.DataFrame(self.ouch).set_index('ts',drop=True)
        return self.ouch
        
