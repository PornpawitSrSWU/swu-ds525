# Creating and Scheduling Data Pipelines
## Data Diagram
<img src="https://github.com/PornpawitSrSWU/swu-ds525/blob/main/01-data-modelling-i/data%20modeling%20i%20diagram.jpg" height="700" width="1000" >


## Get start
เริ่มต้น
```sh
cd 05-creating-and-scheduling-data-pipelines
```


ถ้าใช้งานระบบที่เป็น Linux ให้เรารันคำสั่งด้านล่างนี้ก่อน

```sh
mkdir -p ./dags ./logs ./plugins
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

หลังจากนั้นให้รัน

```sh
docker-compose up
```

เราจะสามารถเข้าไปที่หน้า Airflow UI ได้ที่ port 8080

เสร็จแล้วให้คัดลอกโฟลเดอร์ `data` ที่เตรียมไว้ข้างนอกสุด เข้ามาใส่ในโฟลเดอร์ `dags` เพื่อที่ Airflow จะได้เห็นไฟล์ข้อมูลเหล่านี้ แล้วจึงค่อยทำโปรเจคต่อ

**หมายเหตุ:** จริง ๆ แล้วเราสามารถเอาโฟลเดอร์ `data` ไว้ที่ไหนก็ได้ที่ Airflow ที่เรารันเข้าถึงได้ แต่เพื่อความง่ายสำหรับโปรเจคนี้ เราจะนำเอาโฟลเดอร์ `data` ไว้ในโฟลเดอร์ `dags` เลย

## config Airflow
 เข้า Airflow แล้ว login
 จากนั้นให้ไปที่ tab admin เลือก  connection
 และเพิ่ม connection
 <img src="https://github.com/PornpawitSrSWU/swu-ds525/blob/main/05-creating-and-scheduling-data-pipelines/Screenshot22.png" height="700" width="1000" >

## data schedule
ให้เข้าที่ tab Dags แล้ว etl โดยเราสามารถตรวจสอบการทำงานได้ในหน้านี้
<img src="https://github.com/PornpawitSrSWU/swu-ds525/blob/main/05-creating-and-scheduling-data-pipelines/Screenshot21.png" height="700" width="1000" >

<img src="https://github.com/PornpawitSrSWU/swu-ds525/blob/main/05-creating-and-scheduling-data-pipelines/Screenshot20.png" height="700" width="1000" >

ซึ่งถ้า schedule ทำงานปกติจะเป็นสีเขียว

## check data
สามารถเช็คว่า data ว่าเข้าสู่ database เราหรือไม่ ให้เข้าที่ postgres
<img src="https://github.com/PornpawitSrSWU/swu-ds525/blob/main/05-creating-and-scheduling-data-pipelines/Screenshot.png" height="700" width="1000" >

<img src="https://github.com/PornpawitSrSWU/swu-ds525/blob/main/05-creating-and-scheduling-data-pipelines/Screenshot16.png" height="700" width="1000" >

<img src="https://github.com/PornpawitSrSWU/swu-ds525/blob/main/05-creating-and-scheduling-data-pipelines/Screenshot17.png" height="700" width="1000" >

<img src="https://github.com/PornpawitSrSWU/swu-ds525/blob/main/05-creating-and-scheduling-data-pipelines/Screenshot18.png" height="700" width="1000" >

<img src="https://github.com/PornpawitSrSWU/swu-ds525/blob/main/05-creating-and-scheduling-data-pipelines/Screenshot19.png" height="700" width="1000" >