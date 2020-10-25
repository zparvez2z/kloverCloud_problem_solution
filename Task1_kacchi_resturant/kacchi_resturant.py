import psycopg2
from psycopg2 import Error


def get_connection():
    try:
        connection = psycopg2.connect(user = "postgres",
                                    password = "postgresPassWord",
                                    host = "127.0.0.1",
                                    port = "5432",
                                    database = "resturant")

        cursor = connection.cursor()
        # Print PostgreSQL Connection properties
        print ( connection.get_dsn_parameters(),"\n")

        # Print PostgreSQL version
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        print("You are connected to - ", record,"\n")
        return connection,cursor

    except (Exception, psycopg2.DatabaseError) as error :
        print ("Error while creating PostgreSQL table", error)

def close_connection(cursor, connection):
                cursor.close()
                connection.close()
                print("PostgreSQL connection is closed")

def create_kacchi_table():
    connection,cursor= get_connection()
 
    try:
        create_table_query = '''CREATE TABLE kacchi
          (id serial PRIMARY KEY,
          name VARCHAR(20) NOT NULL,
          price REAL); '''
    
        cursor.execute(create_table_query)
        connection.commit()
        print("Table created successfully in PostgreSQL ")

    except (Exception, psycopg2.DatabaseError) as error :
        print ("Error while creating PostgreSQL table", error)

    finally:
        if(connection):
            close_connection(cursor,connection)

def create_sellData_table():
    connection,cursor= get_connection()
    cursor = connection.cursor()
   
    try:
        create_table_query = '''CREATE TABLE sell_data
          (sell_id bigserial PRIMARY KEY,
          item_id serial,
          number_of_plates INT NOT NULL,
          total_price REAL,
          date_time TIMESTAMP,
          CONSTRAINT fk_kacchi
            FOREIGN KEY(item_id)
                REFERENCES kacchi(id)          
          ); '''
    
        cursor.execute(create_table_query)
        connection.commit()
        print("Table created successfully in PostgreSQL ")

    except (Exception, psycopg2.DatabaseError) as error :
        print ("Error while creating PostgreSQL table", error)
    
    finally:
        if(connection):
            close_connection(cursor,connection)

def create_the_tables():
  create_kacchi_table()
  create_sellData_table()

def add_item( name, price ):
    connection,cursor= get_connection()
    try:
      postgres_insert_query = """ INSERT INTO kacchi (name, price) VALUES (%s,%s)"""
      record_to_insert = (name, price)
      cursor.execute(postgres_insert_query, record_to_insert)

      connection.commit()
      count = cursor.rowcount
      print (count, "Record inserted successfully into kacchi table")

    except (Exception, psycopg2.Error) as error :
        if(connection):
            print("Failed to insert record into kacchi table", error)

    finally:
        if(connection):
            close_connection(cursor,connection)

def update_item_name( id, name):
    connection,cursor= get_connection()
    try:
        print("Table Before updating record ")
        sql_select_query = """SELECT * FROM kacchi where id = %s"""
        cursor.execute(sql_select_query, (id, ))
        record = cursor.fetchone()
        print(record)

        sql_update_query = """Update kacchi SET NAME = %s WHERE id = %s"""
        cursor.execute(sql_update_query, (name, id))
        connection.commit()
        count = cursor.rowcount
        print(count, "Record Updated successfully ")

        print("Table After updating record ")
        sql_select_query = """SELECT * FROM kacchi WHERE id = %s"""
        cursor.execute(sql_select_query, (id,))
        record = cursor.fetchone()
        print(record)

    except (Exception, psycopg2.Error) as error:
        print("Error in update operation", error)

    finally:
        if(connection):
            close_connection(cursor,connection)

def update_item_price( id, price):
    connection,cursor= get_connection()
    try:
        print("Table Before updating record ")
        sql_select_query = """SELECT * FROM kacchi WHERE id = %s"""
        cursor.execute(sql_select_query, (id, ))
        record = cursor.fetchone()
        print(record)

        sql_update_query = """UPDATE kacchi set price = %s WHERE id = %s"""
        cursor.execute(sql_update_query, (price, id))
        connection.commit()
        count = cursor.rowcount
        print(count, "Record Updated successfully ")

        print("Table After updating record ")
        sql_select_query = """SELECT * FROM kacchi where id = %s"""
        cursor.execute(sql_select_query, (id,))
        record = cursor.fetchone()
        print(record)

    except (Exception, psycopg2.Error) as error:
        print("Error in update operation", error)

    finally:
        if(connection):
            close_connection(cursor,connection)

def delete_item(id):
    connection,cursor= get_connection()
    try:
        sql_delete_query = """DELETE FROM kacchi WHERE id = %s"""
        cursor.execute(sql_delete_query, (id, ))
        connection.commit()
        count = cursor.rowcount
        print(count, "Record deleted successfully ")

    except (Exception, psycopg2.Error) as error:
        print("Error in Delete operation", error)
    finally:
        if(connection):
            close_connection(cursor,connection)

def store_sell_data(item_id, number_of_plates):
    connection,cursor= get_connection()
    try:
      sql_select_query = """SELECT price FROM kacchi WHERE id = %s"""
      cursor.execute(sql_select_query, (item_id, ))
      record = cursor.fetchone()
      item_price = record[0]

      total_price = int(number_of_plates)*float(item_price)
      print("total price: {}".format(total_price))

      postgres_insert_query = """ INSERT INTO sell_data (item_id, number_of_plates,total_price,date_time) VALUES (%s,%s,%s,current_timestamp)"""
      record_to_insert = (item_id, number_of_plates, total_price)
      cursor.execute(postgres_insert_query, record_to_insert)

      connection.commit()
      count = cursor.rowcount
      print (count, "Record inserted successfully into sell_data table")

    except (Exception, psycopg2.Error) as error :
        if(connection):
            print("Failed to insert record into sell_data table", error)

    finally:
        #closing database connection.
        if(connection):
            close_connection(cursor,connection)

def get_todays_sell_data():
  connection,cursor= get_connection()
  try:
        print("Todays sell data:")
        
        sql_select_query = """SELECT SUM(number_of_plates) FROM sell_data WHERE DATE(date_time) = current_date;"""
        cursor.execute(sql_select_query)
        record = cursor.fetchone()
        total_number_of_plates = record[0]

        sql_select_query = """SELECT SUM(total_price) FROM sell_data WHERE DATE(date_time) = current_date;"""
        cursor.execute(sql_select_query)
        record = cursor.fetchone()
        total_sell_amount = record[0]

        print("Total plates sold: {} and Total amount: {}".format(total_number_of_plates,total_sell_amount))

  except (Exception, psycopg2.Error) as error:
      print("some error occured", error)

  finally:
      #closing database connection.
      if(connection):
          close_connection(cursor,connection)



if __name__ == "__main__":
  create_the_tables()
  add_item("Beef kacchi half",90)
  add_item("Beef kacchi full",180)
  add_item("Mutton kacchi half",100)
  add_item("Mutton kacchi full",200)
  add_item("Mutton kacchi quarter",40)

  update_item_price(5,50)

  delete_item(5)

  store_sell_data(1,4)
  store_sell_data(2,2)
  store_sell_data(3,3)
  store_sell_data(4,1)

  get_todays_sell_data()


