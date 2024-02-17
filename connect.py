import psycopg2
# from config import load_config

def connect():
    """ Connect to the PostgreSQL database server """
    global conn
    try:
        # connecting to the PostgreSQL server
        with psycopg2.connect("dbname=rinha user=rinha password=rinha") as conn:
            print('Connected to the PostgreSQL server.')
            # set autocommit True to work with PostgreSQL functions
            conn.set_session( autocommit=True)
            return conn
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)

def transaction(id: int, vlr: int, msg: str, type: str):
    with conn.cursor() as cur:
        tran = 'SELECT * FROM debitar(%s,%s,%s)' if type == 'd' else 'SELECT * FROM creditar(%s,%s,%s)'
        try:
            cur.execute(tran, (id, vlr,msg))
        except (psycopg2.DatabaseError, Exception) as error:
            print('erro',error)
            return (0,0,'i')
        ret = cur.fetchone()
        # if None client inexist
        if ret==None:
            return (0,0,'i')
        if len(ret) > 0:
            saldo = ret[0]
            limite = ret[1]
            msg =ret[3]
    return(saldo,limite, msg)

def balance(id: int):
    with conn.cursor() as cur:
        tran = 'SELECT * FROM obter_extrato(%s)'
        # try:
        cur.execute(tran, (id,))
        # except (psycopg2.DatabaseError, Exception) as error:
            # print(error)

        ret = cur.fetchone()[0]
    return(ret)

#if __name__ == '__main__':
# config = load_config()
connect()