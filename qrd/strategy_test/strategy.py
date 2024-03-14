from sortedcontainers import SortedDict
import pandas as pd 
import qrd.data.utils as utils

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
    
    def add_data(self,data : pd.DataFrame):
        if type(data.index)!=pd.core.indexes.datetimes.DatetimeIndex:
            raise ValueError('index must be datetime')
        if 'messages' not in data.columns:
            #maybe more checks later
            raise ValueError('messages column not found')
        msgs=utils.extract_messages(data['messages'])
        self.data=msgs.join(data).drop(columns=['messages'])
        self.data['ts']=self.data.index

    #this will be overrided by the user
    def on_mold_update(self,row):
        raise NotImplementedError('on_mold_update must be implemented')

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

                
        elif row.Type=='E':
            px=row.px
            #self.qty_by_px[px]-=row.qty
            if row.Direction=='B':
                self.bidqty_by_px[px]-=row.qty
                qty_to_be_executed=row.qty
                while len(self.bids)>0 and px <= self.bids.peekitem(-1)[0] and qty_to_be_executed>0 and len(self.bids.peekitem(-1)[1])>0:
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
                    if len(self.bids[best_bid_px])==0:
                        self.bids.pop(best_bid_px)

            else:
                qty_to_be_executed=row.qty
                self.askqty_by_px[px]-=row.qty
                while len(self.offers)>0 and px >= self.offers.peekitem(0)[0] and qty_to_be_executed>0 and len(self.offers.peekitem(0)[1])>0:
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
                
                    if len(self.offers[best_offer_px])==0:
                        self.offers.pop(best_offer_px)        

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
    
    def run(self):
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
            if row.ts!=prev_ts:
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
                    
                    #handling deletions first
                    if order.type=='D':
                        if order.side=='B':
                            found=False
                            if order.price in self.bids:
                                for i in range(len(self.bids[order.price])):
                                    if self.bids[order.price][i].order_id==order.order_id:
                                        self.bids[order.price].pop(i)
                                        found=True
                                        break
                            
                            if not found:
                                print('order to be deleted not found with id:',order.order_id)
                        else:
                            found=False
                            if order.price in self.offers:
                                for i in range(len(self.offers[order.price])):
                                    if self.offers[order.price][i].order_id==order.order_id:
                                        self.offers[order.price].pop(i)
                                        found=True
                                        break
                            if not found:
                                #raise ValueError('order to be deleted not found with id:',order.order_id)
                                print('order to be deleted not found with id:',order.order_id)
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
                        elif order.price in self.offers:
                            self.offers[order.price].append(order)
                        else:
                            self.offers[order.price]=[order]

                self.on_mold_update(row)

            prev_ts=row.ts
            self.prev_row=row
        return pd.DataFrame(self.ouch).set_index('ts',drop=True)
        
