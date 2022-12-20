select DATE_PART(month, order_date)as Month ,pizza_name, SUM(quantity) as number_of_sale 
from pizzasale 
group by DATE_PART(month, order_date), pizza_name 
ORDER BY DATE_PART(month, order_date), number_of_sale DESC