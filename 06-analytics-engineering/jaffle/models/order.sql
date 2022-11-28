select
        o.id
        , o.order_date
        , o.status
        , c.firstname
        , c.last_name
        
from jaffle_shop_order as o
join jaffle_shop_customers as c
on
    o.user_id = c.id
where status = 'completed'