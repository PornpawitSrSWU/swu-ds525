select extract(hour from order_time) as hours,pizza_name, SUM(quantity) as number_of_sale 
from pizzasale 
group by extract(hour from order_time) , pizza_name 
ORDER BY extract(hour from order_time), number_of_sale DESC