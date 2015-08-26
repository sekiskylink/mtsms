-- individual
DROP VIEW IF EXISTS outgoing_messages;
CREATE VIEW outgoing_messages AS
SELECT
    a.id, a.connection_id, a.text, a.status, a.date, a.direction,
    a.priority, a.batch_id,
    b.identity, c.name AS backend

FROM
    rapidsms_httprouter_message a,
    rapidsms_connection b,
    rapidsms_backend c
    -- rapidsms_httprouter_messagebatch d
WHERE
    a.direction = 'O'
    AND
    a.status = 'Q'
    AND
    a.text <> ''
    AND
    a.connection_id = b.id
    AND
    b.backend_id = c.id
    AND
    b.identity ~ '^256(3[19]|41|7[015789])[0-9]{7}$'
    -- AND backend IN ('yo')
    ;


-- outgoing_batch_messages
