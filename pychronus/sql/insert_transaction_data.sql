insert into staging.transactions
SELECT rw.transaction_uuid, rw.transaction_date, rw.post_date, rw.bank, rw."type", rw.category, rw.vendor, rw.description, rw.amount
FROM raw.transactions rw
left join staging.transactions stg
on rw.transaction_uuid = stg.transaction_uuid
where stg.transaction_uuid is null
