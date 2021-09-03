import psycopg2

try:
    connection = psycopg2.connect(user="postgres",
                                  password="Test1236",
                                  host="localhost",
                                  port="5432",
                                  database="postgres")
    cursor = connection.cursor()
    postgreSQL_select_Query = "select * from users"

    cursor.execute(postgreSQL_select_Query)
    print("Selecting rows from users table using cursor.fetchall")
    user_records = cursor.fetchall()

    print("Print each row and it's columns values")
    for row in user_records:
        print("Id = ", row[0], )
        print("First Name = ", row[1])
        print("Last Name = ", row[2])
        print("Gender  = ", row[3], "\n")

except (Exception, psycopg2.Error) as error:
    print("Error while fetching data from PostgreSQL", error)

finally:
    # closing database connection.
    if connection:
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")