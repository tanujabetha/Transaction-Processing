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
# ADD A USER
########################################################################################################################################################
########################################################################################################################################################
########################################################################################################################################################
########################################################################################################################################################
class Transaction1(Thread):
    latencies = []
    def __init__(self,  userId, userName, email, password, name = "Transaction-1"):
        Thread.__init__(self,name=name)
        self.userId = userId
        self.userName = userName
        self.email = email
        self.password = password
        self.total_time = 0.0
        self.client = MongoClient("mongodb://localhost:27017")
        self.db1 = self.client.UserDatabase

    def t1_hop1(self):
        hop_start = time.perf_counter()
        node1_queue.append('T1')
        print(f"\nTransaction_1 : Hop_1 - Started. \n Node_1 at the start of Transaction_1 : {node1_queue} \n Node_2 at the start of Transaction_1 : {node2_queue} \n Node_3 at the start of Transaction_1 : {node3_queue}")
        try:
            existing_user = self.db1.users.find_one({"$or": [{"user_id": self.userId}, {"email": self.email}]})
            if existing_user:
                print(f"\nTransaction_1 : The user already exists.")
                print(f"\nTransaction_1 : Hop_1 - Successful - No need of further hops.")
                node1_queue.remove('T1')
                return
            user = {
                "user_id": self.userId,
                "username": self.userName,
                "email": self.email,
                "password": self.password,
                "created_at": datetime.datetime.now()
            }
            self.db1.users.insert_one(user)
            print(f"\nTransaction_1 : Hop_1 - Successful.")
            hop_end = time.perf_counter()
            node1_queue.remove('T1')
            h1_latency = hop_end - hop_start
            Transaction1.latencies.append(h1_latency)
            node1['Transaction_1'] = h1_latency
        except Exception as e:
            print(f"\nTransaction_1 : Hop_1 - Aborts - {e}")

    def run(self):
        start_time = time.perf_counter()
        runningTransactions.append('T1')
        print(f"\nTransaction_1 : Started")
        self.t1_hop1()
        end_time = time.perf_counter()
        total_time = end_time - start_time
        runningTransactions.remove('T1')
        print(f"\nTransaction_1 : Ended print \n  Node_1 at the end of Transaction_1 : {node1_queue} \n Node_2 at the start of Transaction_1 : {node2_queue} \n Node_3 at the start of Transaction_1 : {node3_queue} \n Transaction_1 : Total Time - {round(total_time,2)} seconds \n")


#ADD PRODUCT
########################################################################################################################################################
########################################################################################################################################################
########################################################################################################################################################
########################################################################################################################################################

class Transaction2(Thread):
    latencies = []

    def __init__(self, productId, productName, Description, Quantity, Price, SellerId, SellerName,  name = "Transaction-2"):
        Thread.__init__(self, name=name)
        self.productId = productId
        self.productName = productName
        self.Description = Description
        self.Quantity = Quantity
        self.Price = Price
        self.total_time = 0.0
        self.client2 = MongoClient("mongodb://localhost:27018")
        self.client3 = MongoClient("mongodb://localhost:27019")
        self.db2 = self.client2.ProductDatabase
        self.db3 = self.client3.OrderInformation
        self.SellerId = SellerId
        self.SellerName = SellerName

    def t2_hop1(self):
        hop_start = time.perf_counter()
        node2_queue.append('T2')
        node3_queue.append('T2')
        print(f"\nTransaction_2 : Hop_1 - Started \n Node_1 at the start of Transaction_2 : {node1_queue} \n Node_2 at the start of Transaction_2 : {node2_queue} \n Node_3 at the start of Transaction_2 : {node3_queue}")
        runFlag = False
        try:
            while not runFlag:
                if node2_queue[0] == 'T2':
                    exists = self.db2.products.find_one({"product_id": self.productId})
                    if exists:
                        print(f"\nTransaction_2 : Product already exists. \nTransaction_2 : Hop_1 - Successful - No need of further hops.")
                        node2_queue.remove('T2')
                        node3_queue.remove('T2')
                        print(f"\nTransaction_2 : Hop_1 - Successful")
                        hop_end = time.perf_counter()
                        h1_latency = hop_end - hop_start
                        Transaction2.latencies.append(h1_latency)
                        node2['Transaction_2'] = h1_latency
                        break
                    else:
                        product = {
                            "product_id": self.productId,
                            "name": self.productName,
                            "description": self.Description,
                            "price": self.Price,
                            "category": "Electronics",
                            "created_at": datetime.datetime.now(),
                            "quantity": self.Quantity,
                            "SellerId": self.SellerId,
                            "SellerName": self.SellerName
                        }
                        result = self.db2.products.insert_one(product)
                        if result:
                            print(f"\nTransaction_2 : Hop_1 - Successful")
                            node2_queue.pop(0)
                            runFlag = True
                            hop_end = time.perf_counter()
                            h1_latency = hop_end - hop_start
                            Transaction2.latencies.append(h1_latency)
                            node2['Transaction_2'] = h1_latency
                            # time.sleep(3)
                            start_time_h2 = time.perf_counter()
                            while not self.t2_hop2(start_time_h2):
                                continue
                else:
                    print(f"\nTransaction_2 : Hop_1 is waiting for {node2_queue[0]} to complete on node_2!")
                    time.sleep(1)
        except Exception as e:
            print(f"\nTransaction_2 : Hop_1 - Aborts - {e}")
            hop_end = time.perf_counter()
            h1_latency = hop_end - hop_start
            node2_queue.pop(0)
            Transaction2.latencies.append(h1_latency)
            node2['Transaction_2'] = h1_latency

    def t2_hop2(self, start_time_h2):
        runFlag = False
        while not runFlag:
            if node3_queue[0] == 'T2':
                print(f"\nTransaction_2 : Hop_2 - Started")
                try:
                    product = {
                        "product_id": self.productId,
                        "quantity": self.Quantity,
                    }
                    result = self.db3.inventory.insert_one(product)
                    if result:
                        print(f"\nTransaction_2 : Hop_2 - Successful")
                        T2_hop2_Time = time.perf_counter()
                        h2_latency = T2_hop2_Time - start_time_h2
                        Transaction2.latencies.append(h2_latency)
                        node3['Transaction_2'] = h2_latency
                        node3_queue.pop(0)
                        runFlag = True
                        return True
                except Exception as e:
                    T2_hop2_Time = time.perf_counter()
                    node3_queue.pop(0)
                    print(f"\nTransaction_2 : Hop_2 did not commit, re-run: {e}")
                    return False
            else:
                print(f"\nTransaction_2 : Hop_2 - Waiting for {node3_queue[0]} to complete on node_3!")
                time.sleep(1)
            return False

    def run(self):
        start_time = time.perf_counter()
        runningTransactions.append('T2')
        print(f"\nTransaction_2 : Started")
        self.t2_hop1()
        end_time = time.perf_counter()
        total_time = end_time - start_time
        runningTransactions.remove('T2')
        print(f"\nTransaction_2 : Ended \n Node_1 at the end of transaction_2 : {node1_queue} \n Node_2 at the end of transaction_2 : {node2_queue} \n Node_3 at the end of transaction_2 : {node3_queue} \n Transaction_2 : Total Time -  {round(total_time, 2)} seconds")

#INCREMENT PRODUCT QUANTITY
########################################################################################################################################################
########################################################################################################################################################
########################################################################################################################################################
########################################################################################################################################################
class Transaction3(Thread):
    latencies = []

    def __init__(self, productId, Quantity,  name = "Transaction-3"):
        Thread.__init__(self, name=name)
        self.productId = productId
        self.Quantity = Quantity
        self.total_time = 0.0
        self.client2 = MongoClient("mongodb://localhost:27018")
        self.client3 = MongoClient("mongodb://localhost:27019")
        self.db2 = self.client2.ProductDatabase
        self.db3 = self.client3.OrderInformation

    def t3_hop1(self):
        hop_start = time.perf_counter()
        node2_queue.append('T3')
        node3_queue.append('T3')
        print(f"\nTransaction_3 : Hop_1 - Started \n Node_1 at the start of Transaction_3 : {node1_queue} \n Node_2 at the start of Transaction_3 : {node2_queue} \n Node_3 at the start of Transaction_3 : {node3_queue}")
        runFlag = False
        try:
            while not runFlag:
                if node2_queue[0] == 'T3':
                    exists = self.db2.products.find_one({"product_id": self.productId})
                    if not exists:
                        print(f"\nTransaction_3 : Product does not exist \n Transaction_3 : Hop_1 - Successful - No need to execute Hop_2")
                        node2_queue.remove('T3')
                        node3_queue.remove('T3')
                        break
                    previous_val = exists['quantity']
                    result = self.db2.products.update_one(
                        {"product_id": self.productId}, {"$set": {"quantity": self.Quantity}}
                    )
                    if result:
                        print(f"\nTransaction_3 : The previous value is {previous_val} and the current value is {self.Quantity}")
                        print(f"\nTransaction_3 : Hop_1 - Successful")
                        runFlag = True
                        hop_end = time.perf_counter()
                        node2_queue.pop(0)
                        h1_latency = hop_end - hop_start
                        Transaction3.latencies.append(h1_latency)
                        node2['Transaction_3'] = h1_latency
                        start_time_h2 = time.perf_counter()
                        while not self.t3_hop2(start_time_h2):
                            continue
                else:
                    print(f"\nTransaction_3 : Hop_1 is waiting for {node2_queue[0]} to complete on node_2!")
                    time.sleep(1)
        except Exception as e:
            print(f"\nTransaction_3 : Hop_1 - Aborts - {e}")

    def t3_hop2(self, hop_start):
        runFlag = False
        while not runFlag:
            if node3_queue[0] == 'T3':
                print('Transaction_3 : Hop_2 - Started')
                try:
                    result = self.db3.inventory.update_one(
                        {"product_id": self.productId}, {"$set": {"quantity": self.Quantity}}
                    )
                    if result:
                        print(f"\nTransaction_3 : Hop_2 - Successful")
                        hop_end = time.perf_counter()
                        h2_latency = hop_end - hop_start
                        Transaction3.latencies.append(h2_latency)
                        node3['Transaction_3'] = h2_latency
                        node3_queue.pop(0)
                        runFlag = True
                        return True
                except Exception as e:
                    print(f"\nTransaction_3 : Hop_2 - Aborts, re-run: {e}")
                    return False
            else:
                print(f"\nTransaction_3 : Hop_2 is waiting for {node3_queue[0]} to complete on node_3")
                time.sleep(1)
            return False

    def run(self):
        print(f"\nTransaction_3 : Started")
        runningTransactions.append('T3')
        start_time = time.perf_counter()
        self.t3_hop1()
        end_time = time.perf_counter()
        self.total_time = end_time - start_time
        runningTransactions.remove('T3')
        print(f"\nTransaction_3 : Ended \n Node_1 at the end of Transaction_3 : {node1_queue} \n Node_2 at the end of Transaction_3 : {node2_queue} \n Node_3 at the end of Transaction_3 : {node3_queue} \n Transaction_3 : Total Time - {round(self.total_time, 2)} seconds")     


#PLACE ORDER    
########################################################################################################################################################
########################################################################################################################################################
########################################################################################################################################################
########################################################################################################################################################       
class Transaction4(Thread):
    latencies = []
    def __init__(self, order_id, user_id, product_id, quantity,  name = "Transaction-4"):
        Thread.__init__(self, name=name)
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

# Transaction_5 : Updating the user's email in the database. 
########################################################################################################################################################
########################################################################################################################################################
########################################################################################################################################################
########################################################################################################################################################   
class Transaction5(Thread):
    latencies = []
    def __init__(self, user_id, new_email, name ="Transaction-5"):
        Thread.__init__(self, name=name)
        self.user_id = user_id
        self.new_email = new_email
        self.total_time = 0.0
        self.client = MongoClient("mongodb://localhost:27017")
        self.db1 = self.client.UserDatabase
        
    def t5_hop1(self):
        hop_start = time.perf_counter()
        node1_queue.append('T5')
        print(f"\nTransaction_5 : Hop_1 - Started  \n Node_1 at the start of Transaction_5 : {node1_queue}  \n Node_2 at the start of Transaction_5 : {node2_queue}  \n Node_3 at the start of Transaction_5 : {node3_queue}")
        flag = False
        redFlag = False
        try:
            while not redFlag:
                if node1_queue[0] == 'T5':
                    user = self.db1.users.find_one({"user_id": self.user_id})
                    if not user:
                        print(f"\nT5 - User with user_id {self.user_id} does not exist.")
                        print(f"\nTransaction_5 : Hop_1 - Successful - No need to execute further hops.")
                        node1_queue.pop(0)
                        return True
                    previous_email = user['email']
                    result = self.db1.users.update_one(
                        {"user_id": self.user_id}, {"$set": {"email": self.new_email}}
                    )
                    if result.matched_count:
                        print(f"\nTransaction_5 : Hop_1 - Successful")
                        print(f"\nTransaction_5 : The previous email was {previous_email} and the new email is {self.new_email}")
                        hop1_time = time.perf_counter()
                        h1_latency = hop1_time - hop_start
                        node1_queue.pop(0)
                        Transaction5.latencies.append(h1_latency)
                        node1['Transaction_5'] = h1_latency
                        flag = True
                        redFlag = True
                    else:
                        print(f"\nTransaction_5 is waiting for {node1_queue[0]} to complete at node_1!")
                        time.sleep(1)
        except Exception as e:
            print(f"\nTransaction_5 : Hop_1 - Aborts - {e}")
        return flag

    def run(self):
        print(f"\nTransaction_5 : Started")
        runningTransactions.append('T5')
        start_time = time.perf_counter()
        self.t5_hop1()
        end_time = time.perf_counter()
        runningTransactions.remove('T5')
        self.total_time = end_time - start_time
        print(f"\nTransaction_5 : Ended \n Node_1 at the end of Transaction_5 : {node1_queue} \n Node_2 at the end of Transaction_5 : {node2_queue} \n Node_3 at the end of Transaction_5: {node3_queue} \n Transaction_5 : Total Time - {round(self.total_time, 2)} seconds")
# Transaction_6 : Updating the product's price in the database.
############################################################################################################################################################
############################################################################################################################################################
############################################################################################################################################################
############################################################################################################################################################ 
class Transaction6(Thread):
    latencies = []
    def __init__(self, product_id, new_price,  name = "Transaction-6"):
        Thread.__init__(self, name=name)
        self.product_id = product_id
        self.new_price = new_price
        self.total_time = 0.0
        self.client = MongoClient("mongodb://localhost:27018")
        self.db2 = self.client.ProductDatabase
        
    def t6_hop1(self):
        hop_start = time.perf_counter()
        node2_queue.append('T6')
        print(f"\nTransaction_6 : Hop_1 - Started \n Node_1 at the start of Transaction_6: {node1_queue} \n Node_2 at the start of Transaction_6: {node2_queue} \n Node_3 at the start of Transaction_6: {node3_queue}")
        flag = False
        runFlag = False
        while not runFlag:
            if node2_queue[0] == 'T6':
                try:
                    product = self.db2.products.find_one({"product_id": self.product_id})
                    if not product:
                        print(f"\nTransaction_6 : Product with product_id {self.product_id} does not exist \n Transaction_6 : Hop_1 - Successful - No need to execute further hops.")
                        node2_queue.pop(0)
                        return True
                    previous_price = product['price']
                    result = self.db2.products.update_one(
                        {"product_id": self.product_id}, {"$set": {"price": self.new_price}}
                    )
                    if result.matched_count:
                        print(f"\nT6 - First hop successful - Price updated")
                        print(f"\nThe previous price was {previous_price} and the new price is {self.new_price}")
                        hop1_time = time.perf_counter()
                        h1_latency = hop1_time - hop_start
                        Transaction6.latencies.append(h1_latency)
                        node2_queue.remove('T6')
                        node2['Transaction_6'] = h1_latency
                        flag = True
                        runFlag = True
                except Exception as e:
                    print(f"\nTransaction_6 - Hop_1 : Aborts: {e}")
                return flag
            else: 
                print(f"\nTransaction_6 is waiting for {node2_queue[0]} to complete on node_2!")
                time.sleep(1)

    def run(self):
        print(f"\nTransaction_6 : Started")
        runningTransactions.append('T6')
        start_time = time.perf_counter()
        self.t6_hop1()
        end_time = time.perf_counter()
        runningTransactions.remove('T6')
        self.total_time = end_time - start_time
        print(f"\nTransaction_6 : Ended \n Node_1 at the end of Transaction_6: {node1_queue} \n Node_2 at the end of Transaction_6: {node2_queue} \n Node_3 at the end of Transaction_6: {node3_queue} \n Transaction_6 : Total Time - {round(self.total_time, 2)} seconds")
        

###########################################################################################################################################################
###########################################################################################################################################################
# # ###########################################################################################################################################################
class Transaction7(Thread):
    latencies = []
    def __init__(self, productId, new_quantity, new_price, name = "Transaction-7"):
        Thread.__init__(self, name=name)
        self.productId = productId
        self.new_quantity = new_quantity
        self.new_price = new_price
        self.total_time = 0.0
        self.client2 = MongoClient("mongodb://localhost:27018")
        self.client3 = MongoClient("mongodb://localhost:27019")
        self.db2 = self.client2.ProductDatabase
        self.db3 = self.client3.OrderInformation

    def t7_hop1(self):
        hop_start = time.perf_counter()
        node2_queue.append('T7')
        node3_queue.append('T7')
        print(f"\nTransaction_7 : Hop_1 - Started \n Node_1 at the start of Transaction_7 : {node1_queue} \n Node_2 at the start of Transaction_7 : {node2_queue} \n Node_3 at the start of Transaction_7 : {node3_queue}")
        runFlag = False
        try:
            while not runFlag:
                if node2_queue[0] == 'T7':
                    product = self.db2.products.find_one({"product_id": self.productId})
                    if not product:
                        print(f"\nTransaction_7 : Product with product_id {self.productId} does not exist \n Transaction_7 : Hop_1 Successful - No need to run further hops.")
                        node2_queue.remove('T7')
                        node3_queue.remove('T7')
                        break
                    else:
                        previous_quantity = product['quantity']
                        previous_price = product['price']
                        result = self.db2.products.update_one(
                            {"product_id": self.productId}, {"$set": {"quantity": self.new_quantity, "price": self.new_price}}
                        )
                        if result.matched_count:
                            print(f"\nTransaction_7 : Hop_1 - Successful - Product info updated")
                            print(f"The previous quantity was {previous_quantity} and the new quantity is {self.new_quantity}")
                            print(f"The previous price was {previous_price} and the new price is {self.new_price}")
                            node2_queue.pop(0)
                            runFlag = True
                            hop_end = time.perf_counter()
                            h1_latency = hop_end - hop_start
                            Transaction7.latencies.append(h1_latency)
                            node2['Transaction_7'] = h1_latency
                            start_time_h2 = time.perf_counter()
                            while not self.t7_hop2(start_time_h2):
                                continue
                else:
                    print(f"\nTransaction_7 : Hop_1 is waiting for {node2_queue[0]} to complete on node_2!")
                    time.sleep(1)
        except Exception as e:
            print(f"\nTransaction_7 : Hop_1 - Aborts - {e}")
            hop_end = time.perf_counter()
            h1_latency = hop_end - hop_start
            node2_queue.pop(0)
            Transaction7.latencies.append(h1_latency)
            node2['Transaction_7'] = h1_latency

    def t7_hop2(self, start_time_h2):
        runFlag = False
        while not runFlag:
            if node3_queue[0] == 'T7':
                print(f"\nTransaction_7 : Hop_2 - Started")
                try:
                    result = self.db3.inventory.update_one(
                        {"product_id": self.productId}, {"$set": {"quantity": self.new_quantity}}
                    )
                    if result.matched_count:
                        print(f"\nTransaction_7 : Hop_2 - Successful - Inventory updated")
                        hop2_time = time.perf_counter()
                        h2_latency = hop2_time - start_time_h2
                        Transaction7.latencies.append(h2_latency)
                        node3['Transaction_7'] = h2_latency
                        node3_queue.pop(0)
                        runFlag = True
                        return True
                except Exception as e:
                    hop2_time = time.perf_counter()
                    node3_queue.pop(0)
                    print(f"\nTransaction_7 : Hop_2 did not commit, re-run: {e}")
                    return False
            else:
                print(f"\nTransaction_7 : Hop_2 - Waiting for {node3_queue[0]} to complete on node_3!")
                time.sleep(1)
            return False

    def run(self):
        runFlag = False
        start_time = time.perf_counter()
        while not runFlag:
            if runningTransactions == []:
                print(f"\nTransaction_7 : Started")
                self.t7_hop1()
                end_time = time.perf_counter()
                total_time = end_time - start_time
                self.total_time = total_time
                runFlag = True
                print(f"\nTransaction_7 : Ended \n Node_1 at the end of Transaction_7 : {node1_queue} \n Node_2 at the end of Transaction_7 : {node2_queue} \n Node_3 at the end of Transaction_7 : {node3_queue} \n Transaction_7 : Total Time -  {round(total_time, 2)} seconds")
            else:
                print(f"\nTransaction_7 : Waiting!")
                time.sleep(1)

# Example Usage




###########################################################################################################################################################
###########################################################################################################################################################
###########################################################################################################################################################
# Initialize transactions
#Add User
userId = random.randint(1,100)
sellerId = random.randint(1,100)

transaction_1 = Transaction1(userId, "Mark", "martin@uci.edu", "password", name = "Transaction-1")
transaction_2 = Transaction2(random.randint(1,100), "Surface", "15.6", 30, 1300, sellerId, "Jacob", name = "Transaction-2")
transaction_3 = Transaction3( 561, random.randint(1,50), name = "Transaction-3")
transaction_4 = Transaction4(order_id=random.randint(1,100), user_id= userId , product_id = 1, quantity = random.randint(1,50), name = "Transaction-4")
transaction_5 = Transaction5(user_id= userId, new_email="tanuja@example.com", name = "Transaction-5")
transaction_6 = Transaction6(product_id=1, new_price=random.randint(100,2000), name = "Transaction-6")
transaction_7 = Transaction7( productId=1, new_quantity=100, new_price=1500.00, name= "Transaction-7")


transaction_1.start()
transaction_2.start()
transaction_3.start()
transaction_4.start()
transaction_5.start()
transaction_6.start()
transaction_7.start()

transaction_1.join()
transaction_2.join()
transaction_3.join()
transaction_4.join()
transaction_5.join()
transaction_6.join()
transaction_7.join()


print(f'\n Hop execution time by Transaction_1 : {transaction_1.total_time}')
print(f'\n Hop execution time by Transaction_2 : {transaction_2.latencies}')
print(f'\n Hop execution time by Transaction_3 : {transaction_3.latencies}')
print(f'\n Hop execution time by Transaction_4 : {transaction_4.latencies}')
print(f'\n Hop execution time by Transaction_5 : {transaction_5.latencies}')
print(f'\n Hop execution time by Transaction_6 : {transaction_6.latencies}')
print(f'\n Hop execution time by Transaction_6 : {transaction_7.latencies}')



print(f"\nHops execution on Node 1 : {node1}")
print(f"\nHops execution on Node 2 : {node2}")
print(f"\nHops execution on Node 3 : {node3}")
# # Calculate and display average latency and throughput
def calculate_metrics(transaction_class):
    total_time = sum(transaction_class.latencies)
    avg_latency = total_time / len(transaction_class.latencies) if transaction_class.latencies else 0
    throughput = len(transaction_class.latencies) / total_time if total_time > 0 else 0
    return avg_latency, throughput

# Calculate metrics for each transaction
avg_latency1, throughput1 = calculate_metrics(Transaction1)
avg_latency2, throughput2 = calculate_metrics(Transaction2)
avg_latency3, throughput3 = calculate_metrics(Transaction3)
avg_latency4, throughput4 = calculate_metrics(Transaction4)
avg_latency5, throughput5 = calculate_metrics(Transaction5)
avg_latency6, throughput6 = calculate_metrics(Transaction6)
avg_latency7, throughput7 = calculate_metrics(Transaction7)

# Print the metrics
print(f"\nTransaction 1 - Average Latency: {avg_latency1:.2f} seconds, Throughput: {throughput1:.2f} transactions/second")
print(f"\nTransaction 2 - Average Latency: {avg_latency2:.2f} seconds, Throughput: {throughput2:.2f} transactions/second")
print(f"\nTransaction 3 - Average Latency: {avg_latency3:.2f} seconds, Throughput: {throughput3:.2f} transactions/second")
print(f"\nTransaction 4 - Average Latency: {avg_latency4:.2f} seconds, Throughput: {throughput4:.2f} transactions/second")
print(f"\nTransaction 5 - Average Latency: {avg_latency5:.2f} seconds, Throughput: {throughput5:.2f} transactions/second")
print(f"\nTransaction 6 - Average Latency: {avg_latency6:.2f} seconds, Throughput: {throughput6:.2f} transactions/second")
print(f"\nTransaction 7 - Average Latency: {avg_latency7:.2f} seconds, Throughput: {throughput7:.2f} transactions/second")
