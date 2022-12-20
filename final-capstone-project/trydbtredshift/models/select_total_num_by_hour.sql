select extract(hour from order_time) as hours, SUM(quantity) as number_of_sale 
from pizzasale 
group by extract(hour from order_time) 
ORDER BY extract(hour from order_time), number_of_sale DESC