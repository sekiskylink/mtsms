import requests
import psycopg2
import psycopg2.extras
import time
import logging

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(process)d] %(levelname)-4s:  %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    # filename='/var/log/mtsms/mtsms.log',
    filename='/tmp/mtsms.log',
    filemode='a'
)

CHUNK_SIZE = 400
ROUTER_URL = {
    'yo8200': 'http://127.0.0.1:13013/cgi-bin/sendsms?from=8200&username=mtrack&password=pae7Phee',
    'yo': 'http://127.0.0.1:13013/cgi-bin/sendsms?from=6767&username=mtrack&password=pae7Phee'
}

DBS = [
    {
        'name': 'mtrack',
        'host': 'localhost',
        'user': 'postgres',
        'passwd': 'postgres',
        'port': '5432'
    },
]

SUPPORTED_BACKENDS = ["yo", "yo8200"]

MESSAGE_TABLE = "rapidsms_httprouter_message"
BATCH_TABLE = "rapidsms_httprouter_messagebatch"


def call_url(url, params={}):
    r = requests.get(url, params=params)
    return r.status_code


def update_status(cur, table, id, status):
    cur.execute("UPDATE %s SET status = '%s' WHERE id = %s" % (table, status, id))


def mass_update(conn, cur, table, ids, status):
    cur.execute(
        "UPDATE %s SET status = '%s' WHERE id IN(%s)" %
        (table, status, ','.join(['%s' % i for i in ids])))
    conn.commit()


def cancel_fake_messages(con):
    """set status of empty messages to 'C' """
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(
        "UPDATE rapidsms_httprouter_message SET status = 'C' "
        "WHERE length(trim(text)) = 0")
    conn.commit()


def cancel_blocking_batches(conn):
    """Cancels any blocking batches and returns ready batches
    """
    to_process = []
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(
        "SELECT id FROM rapidsms_httprouter_messagebatch WHERE status = %s ORDER BY id", ('Q'))
    res = cur.fetchall()
    if res:
        for r in res:
            # check for any queued messages in batch
            cur.execute(
                "SELECT count(id) as num FROM rapidsms_httprouter_message WHERE batch_id = %s "
                "AND status = %s", (r["id"], 'Q'))
            r1 = cur.fetchone()
            if r1["num"] == 0:  # blocking
                update_status(cur, BATCH_TABLE, r["id"], 'C')
                conn.commit()
            else:
                to_process.append(r["id"])
    return to_process


def send_individual(conn):
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(
        "SELECT id, text, identity, priority, backend FROM outgoing_messages ORDER BY priority, id")  # FIFO if no priority
    res = cur.fetchall()
    if res:
        for r in res:
            params = {
                "to": r["identity"],
                "text": r["text"],
                "smsc": "yo" if r["backend"] == "yo8200" else "yo",
                'dlr-url': 'http://rapidsms.org/router/delivered/?message_id=%s' % r["id"],
                'drl-mask': 1,
            }  # XXX url quote text
            if r["backend"] in SUPPORTED_BACKENDS:  # Only send to supported backends -> limit in DB too!
                # sendurl = ROUTER_URL % params
                # print sendurl
                try:
                    status_code = call_url(ROUTER_URL[r["backend"]], params)
                    cur.execute("SELECT id FROM rapidsms_httprouter_message WHERE id = %s FOR UPDATE NOWAIT" % r["id"])
                    if int(status_code / 100) == 2:  # 2xx = Ok for Kannel
                        update_status(cur, MESSAGE_TABLE, r["id"], 'S')
                        conn.commit()
                    elif status_code == 403:
                        update_status(cur, MESSAGE_TABLE, r["id"], 'K')
                        conn.commit()
                    else:
                        pass
                except Exception as e:
                    logging.error("SMS:%s Message not sent: %s" % (r["id"], str(e)))
            else:  # unsupported backend
                # log and continue
                logging.error("SMS:%s has unsupported backend:%s" % (r["id"], r["backend"]))
                update_status(cur, MESSAGE_TABLE, r["id"], 'C')
                conn.commit()
                continue
    else:
        logging.info("No individual messages found for sending")
        print "No individual messages found"


def send_chunk(conn, chunk, batchid):
    if not chunk:
        return
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    # clearly breakdown chunk into messages for different backends
    backend_recipients = {}  # XXX e.g {"yo": {"ids": [], "recipients": [], "message": "xx"}, }
    for m in chunk:
        if m["backend"] not in backend_recipients:
            backend_recipients[m["backend"]] = {
                "ids": [m["id"]], "recipients": [m["identity"]], "message": m["text"]}
        else:
            backend_recipients[m["backend"]]["ids"].append(m["id"])
            backend_recipients[m["backend"]]["recipients"].append(m["identity"])

    # now send messages for each SUPPORTED backend
    for backend, val in backend_recipients.iteritems():
        if backend not in SUPPORTED_BACKENDS:  # do not send to unsupported backend
            mass_update(conn, cur, MESSAGE_TABLE, val["ids"], 'C')
            logging.info("Canceld Batch messages with unsuported backend(%s):%s" % (backend, val["ids"]))
        recipient_list = ' '.join(val["recipients"])
        params = {
            "to": recipient_list,
            "text": val["message"],
            "smsc": "yo" if backend == "yo8200" else "yo",
            'dlr-url': 'http://rapidsms.org/router/delivered/?message_id=%s' % batchid,
            'drl-mask': 1,
        }  # XXX remember to url quote

        # select ids for update
        cur.execute(
            "SELECT id FROM rapidsms_httprouter_message WHERE "
            "id IN (%s) FOR UPDATE NOWAIT" % ','.join(['%s' % i for i in val["ids"]]))
        try:
            status_code = call_url(ROUTER_URL[backend], params)
            if int(status_code / 100) == 2:  # 2xx = Ok for Kannel
                mass_update(conn, cur, MESSAGE_TABLE, val["ids"], 'S')
            elif status_code == 403:
                mass_update(conn, cur, MESSAGE_TABLE, val["ids"], 'K')
            else:
                pass
        except Exception as e:
            logging.error("SMS:%s Message not sent: %s" % (val["ids"], str(e)))
        # update status of messages and batch to S if successful


def process_batches(conn, batches):
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    for batch in batches:
        cur.execute(
            "SELECT id, text, identity, backend FROM outgoing_messages WHERE batch_id = "
            " %s ORDER by priority, id, backend" % batch)
        res = cur.fetchall()
        if res:
            logging.info("Started processing Batch:%s" % batch)
            total = len(res)
            j = 0
            for i in range(0, total + CHUNK_SIZE, CHUNK_SIZE)[1:]:  # want to finsh batch right away
                chunk = res[j:i]
                # do our thing with chunk
                print ">>>> %s <<<<" % chunk
                send_chunk(conn, chunk, batch)
                j = i
            print "Batchid######:%s" % batch
            cur.execute(
                "SELECT count(id) AS total FROM rapidsms_httprouter_message WHERE "
                "batch_id = %s AND status IN ('S', 'C', 'K')" % batch)
            t = cur.fetchone()
            if total == t["total"]:  # batch finished
                update_status(cur, BATCH_TABLE, batch, 'S')
                conn.commit()
                logging.info("Batch:%s successfully completed" % batch)
            else:
                print "=>total1:%s, total2:%s" % (total, t["total"])

while True:
    for db in DBS:
        conn = psycopg2.connect(
            "dbname=" + db["name"] + " host= " + db["host"] + " port=" + db["port"] +
            " user=" + db["user"] + " password=" + db["passwd"])
        # start SMS processing
        cancel_fake_messages(conn)
        batches = cancel_blocking_batches(conn)

        process_batches(conn, batches)

        # send singles
        send_individual(conn)

        conn.close()
        time.sleep(0.5)
