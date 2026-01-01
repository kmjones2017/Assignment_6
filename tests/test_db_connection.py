from tasks.db import get_db_connection

def main():
    conn = get_db_connection()
    print("Connected successfully")
    conn.close()

if __name__ == "__main__":
    main()
