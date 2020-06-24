
DROP TABLE IF EXISTS record;
CREATE TABLE record (
    pk INTEGER PRIMARY KEY AUTOINCREMENT,
    rec_id TEXT UNIQUE,
    rec_type TEXT,
    sqft FLOAT,
    created_date TEXT DEFAULT CURRENT_TIMESTAMP,
    created_user TEXT DEFAULT 'DefaultUser'
    );


DROP TABLE IF EXISTS fees;
CREATE TABLE fees (
    pk INTEGER PRIMARY KEY AUTOINCREMENT,
    rec_id TEXT,
    fee_code TEXT,
    fee FLOAT,
    status TEXT DEFAULT 'Pending',  -- {'Pending', 'Invoiced', 'Waived', 'Paid', 'Late', 'Delinquent'}
    comment TEXT,  -- Required if status == 'Waived'
    created_date TEXT DEFAULT CURRENT_TIMESTAMP
    );

DROP TRIGGER IF EXISTS fee_waive_comment_insert_trg;
CREATE TRIGGER fee_waive_comment_insert_trg
BEFORE INSERT ON fees
WHEN NEW.status = 'Waived'
BEGIN
    SELECT
        CASE
            WHEN NEW.comment IS NULL
            THEN RAISE(ABORT, 'Waived fees must have a comment')
        END;
END;


DROP TRIGGER IF EXISTS fee_waive_comment_update_trg;
CREATE TRIGGER fee_waive_comment_update_trg
BEFORE UPDATE ON fees
WHEN NEW.status = 'Waived'
BEGIN
    SELECT
        CASE
            WHEN NEW.comment IS NULL
            THEN RAISE(ABORT, 'Waived fees must have a comment')
        END;
END;


INSERT INTO fees (rec_id, fee_code, fee) VALUES ('GAR-TEST-001', 'BLD_APP', 42.0);

UPDATE fees
SET status = 'Waived', comment = 'Because reasons'
WHERE pk = 1;
