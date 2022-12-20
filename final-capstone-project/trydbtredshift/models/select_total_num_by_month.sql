select DATE_PART(month, order_date)as Month, SUM(quantity) as number_of_sale 
from pizzasale 
group by DATE_PART(month, order_date) 
ORDER BY DATE_PART(month, order_date), number_of_sale DESC