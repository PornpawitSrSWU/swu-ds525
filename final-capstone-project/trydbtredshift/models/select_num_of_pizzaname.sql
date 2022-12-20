select pizza_name, SUM(quantity) as number_of_sale 
from pizzasale 
group by pizza_name 
ORDER BY number_of_sale DESC