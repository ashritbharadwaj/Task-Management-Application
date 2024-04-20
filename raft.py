import time
import random
import mysql.connector

class Node:
  def __init__(self, id, processors, db_config):
    self.id = id
    self.processors = processors
    self.nodes = None
    self.leader = None
    self.task = None
    self.log = []
    self.task_time = None
    self.db_config = db_config
    self.create_table()
    self.log_file = f"log_{id}.txt"
    with open(self.log_file, "w") as f:
        f.write("")

  def request_consensus(self, task):
    votes = 0
    for p in range(self.processors):
      if p != self.id:
        votes += 1
    if votes > self.processors // 2:
      self.leader = self.id
      self.task = task
      time.sleep(self.task_time)
      for p in self.nodes:
        if p.id != self.id:
            p.write_log(self)
            p.insert_row(self)
      self.log.append(f"Task {task} completed")
      self.write_log(self)
      self.insert_row(self)
      return True
    return False

  def write_log(self,node):
    with open(f"log_{self.id}.txt", "a") as f:
        f.write(f"Node {node.id} completed task: {node.task}\n")

  @staticmethod
  def run(nodes):
    random.shuffle(nodes)
    for node in nodes:
        node.request_consensus(node.task)

  def create_table(self):
      connection = mysql.connector.connect(**self.db_config)
      cursor = connection.cursor()
      try:
        cursor.execute(f"DROP TABLE IF EXISTS processor{self.id}")
        connection.commit()
        cursor.execute(f"CREATE TABLE processor{self.id}(id INT AUTO_INCREMENT PRIMARY KEY, transaction VARCHAR(255) NOT NULL);")
      except mysql.connector.Error as err:
        print(f"Error Creating table: {err}")
      finally:
         if connection:
            cursor.close()
            connection.close()
  
  def insert_row(self,node):
      connection = mysql.connector.connect(**self.db_config)
      cursor = connection.cursor()
      data = (f"Node {node.id} completed the task: {node.task}",)
      # cursor.execute("SHOW TABLES")
      # tables = cursor.fetchall()
      # for table in tables:
      #    print(table)
      try:
          # print(self.id,"--",node.id)
          insert_query = f"INSERT INTO processor{self.id} (transaction) VALUES (%s)"
          cursor.execute(insert_query, data)
          connection.commit()
          # print("inserted successfully")
      except mysql.connector.Error as err:
        print(f"Error Inserting into table: {err}")
      finally:
         if connection:
            cursor.close()
            connection.close()
