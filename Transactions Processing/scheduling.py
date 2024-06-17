import random
import time
from queue import Queue
from pymongo import MongoClient
from threading import Thread, Event
import datetime

node1_queue = []
node2_queue = []
node3_queue = []
node1 = {}
node2 = {}
node3 = {}
runningTransactions = []

#PLACE ORDER    
########################################################################################################################################################
########################################################################################################################################################
########################################################################################################################################################
########################################################################################################################################################       
class Transaction4:
    latencies = []
    def __init__(self, order_id, user_id, product_id, quantity):
        self.order_id = order_id
        self.user_id = user_id
        self.product_id = product_id
        self.quantity = quantity
        self.total_time = 0.0
        self.client2 = MongoClient("mongodb://localhost:27018")
        self.client3 = MongoClient("mongodb://localhost:27019")
        self.db2 = self.client2.ProductDatabase
        self.db3 = self.client3.OrderInformation
        self.products = self.db2.products
        self.orders = self.db3.orders
        self.order_items = self.db3.order_items
        self.inventory = self.db3.inventory
        
    def t4_hop1(self):
        hop_start = time.perf_counter()
        node2_queue.append('T4')
        node3_queue.append('T4')
        print(f"\nTransaction_4 : Hop_1 - Started \n Node_1 at the start of Transaction_4 : {node1_queue} \n Node_2 at the start of Transaction_4 : {node2_queue} \n Node_3 at the start of Transaction_4 : {node3_queue}")
        runFlag = False
        try:
            while not runFlag:
                if node3_queue[0] == 'T4':
                # Check if the product quantity in inventory is sufficient
                    product_in_inventory = self.inventory.find_one({"product_id": self.product_id})
                    if not product_in_inventory or product_in_inventory["quantity"] < self.quantity:
                        print(f"\nTransaction_4 : Order {self.order_id} cannot be placed due to insufficient quantity \n Transaction_4 : Hop_1 - Successful - No need to do Hop_2.")
                        node3_queue.remove('T4')
                        node2_queue.remove('T4')
                        return
                    else:
                        # Decrement the quantity in inventory
                        new_quantity = product_in_inventory["quantity"] - self.quantity
                        self.inventory.update_one({"product_id": self.product_id}, {"$set": {"quantity": new_quantity}})
                        # Add order to Orders collection
                        self.orders.insert_one({
                            "order_id": self.order_id,
                            "user_id": self.user_id,
                            "order_date": datetime.datetime.now(),
                            "status": "Pending"
                        })
                        # Add order item to OrderItems collection
                        self.order_items.insert_one({
                            "order_item_id": self.order_id,
                            "order_id": self.order_id,
                            "product_id": self.product_id,
                            "quantity": self.quantity,
                            "price": self.products.find_one({"product_id": self.product_id})["price"]  
                        })
                        hop1_time = time.perf_counter()
                        h1_latency = hop1_time - hop_start
                        print(f"\nTransaction_4 : Order {self.order_id} has been placed successfully in hop 1. \n Transaction_4 : Hop_1 - Successful")
                        Transaction4.latencies.append(h1_latency)
                        node3['Transaction_4'] = h1_latency
                        node3_queue.remove('T4')
                        runFlag = True
                        start_time = time.perf_counter()
                        while not self.t4_hop2(start_time):
                            continue
                else:
                    print(f"\nTransaction_4 : Hop_1 is waiting for {node3_queue[0]} to complete on node_3!")
                    time.sleep(1)
        except Exception as e:
            print(f"\nTransaction_4 : Hop_1 - Aborts - {e}")


    def t4_hop2(self, start_time):
        runFlag = False    
        print('Transaction_4 : Hop_2 - Started')
        while not runFlag:
            if node2_queue[0] == 'T4':
                try:
                    # Decrement the product quantity in ProductDatabase
                    product = self.products.find_one({"product_id": self.product_id})
                    if product:
                        new_quantity = product["quantity"] - self.quantity
                        self.products.update_one({"product_id": self.product_id}, {"$set": {"quantity": new_quantity}})
                        print(f"\nTransaction_4 : Product {self.product_id} quantity has been decremented by {self.quantity}.")
                        hop2_time = time.perf_counter()
                        node2_queue.remove('T4')
                        print(f"Node_2 : Transaction_4 : {node2_queue}")
                        h2_latency = hop2_time - start_time
                        Transaction4.latencies.append(h2_latency)
                        node2['Transaction_4'] = h2_latency
                        runFlag = True  
                        return True
                    else:
                        print(f"\nProduct {self.product_id} not found in Product Database.")
                        node2_queue.remove('T4')
                        runFlag = True
                        return True
                except Exception as e:
                    print(f"\nTransaction_4 : Hop_2 - Aborts, re-run: {e}")
                    return False
            else:
                print(f"\nTransaction_4 : Hope_2 is waiting for {node2_queue[0]} to complete on node_2!")
                time.sleep(1)
            

    def run(self):
        print(f"\nTransaction_4 : Started")
        runningTransactions.append('T4')
        start_time = time.perf_counter()
        self.t4_hop1()
        end_time = time.perf_counter()
        runningTransactions.remove('T4')
        self.total_time = end_time - start_time
        print(f"\nTransaction_4 : Ended \n Node_1 at the end of Transaction_4 : {node1_queue}\n Node_2 at the end of Transaction_4 : {node2_queue}\n Node_3 at the end of Transaction_4 : {node3_queue} \n Transaction_4 : Total Time - {round(self.total_time, 2)} seconds")
        
        
        
##############################################################################################################################################################################################################################################################################################
#######################################################################################################################################################################################################################################################################################################        
############################################################################################################################################################################################################################################################################################# 
#######################################################################################################################################################################################################################################################################################################       


class Transaction8:
    latencies = []
    def __init__(self, seller_id, name="Transaction-8"):
        Thread.__init__(self, name=name)
        self.seller_id = seller_id
        self.total_time = 0.0
        self.client1 = MongoClient('mongodb://localhost:27017')
        self.client2 = MongoClient("mongodb://localhost:27018")
        self.client3 = MongoClient("mongodb://localhost:27019")
        self.db1 = self.client1.UserDatabase
        self.db2 = self.client2.ProductDatabase
        self.db3 = self.client3.OrderInformation
    def t8_hop1(self):
        hop_start = time.perf_counter()
        print("T8 Hop_1 started")
        try:
            result = self.db1.sellers.delete_many({"seller_id": self.seller_id})
            if result.deleted_count:
                print(f"Seller {self.seller_id} deleted from sellers table")
                hop_end = time.perf_counter()
                h1_latency = hop_end - hop_start
                Transaction8.latencies.append(h1_latency)
                return True
            else:
                print("Seller doesn't exist")
        except Exception as e:
            print(f"Hop 1 - Error occurred: {e}")
        return False

    def t8_hop2(self):
        hop_start = time.perf_counter()
        print("T8 Hop_2 started")
        try:
            products = self.db2.products.find({"seller_id": self.seller_id})
            product_ids = [product["product_id"] for product in products]
            result = self.db2.products.delete_many({"seller_id": self.seller_id})
            if result.deleted_count:
                print(f"{result.deleted_count} products deleted associated with the seller")
                hop_end = time.perf_counter()
                h2_latency = hop_end - hop_start
                Transaction8.latencies.append(h2_latency)
                return True, product_ids
            else:
                print(f"No product associated with the seller exists")
                return False, []
        except Exception as e:
            print(f"Hop 2 - Error occurred: {e}")
            return False, []
    
    def t8_hop3(self, product_ids):
        hop_start = time.perf_counter()
        print("T8 Hop_3 started")
        try:
            result = self.db3.inventory.delete_many({"product_id": {"$in": product_ids}})
            if result.deleted_count:
                print(f"{result.deleted_count} products deleted from inventory associated with the seller")
                hop_end = time.perf_counter()
                h3_latency = hop_end - hop_start
                Transaction8.latencies.append(h3_latency)
                return True
            else:
                print(f"Inventory contains no products associated with the seller's products")
                return False
        except Exception as e:
            print(f"Hop 3 - Error occurred: {e}")
            return False

    def run(self):
        start_time = time.time()
        if self.t8_hop1():
            hop2_successful, product_ids = self.t8_hop2()
            if hop2_successful:
                self.t8_hop3(product_ids)
        end_time = time.time()
        self.total_time = end_time - start_time
        print(f"Total time taken: {self.total_time} seconds")



transaction_4 = Transaction4(3, 2, 5 , 100)
transaction_8 = Transaction8()
transaction_4.t4_hop1()
transaction_8.t8_hop1()