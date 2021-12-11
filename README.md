# Final Project
## Ruthapoom Sibanyen ID:63606009

## Instructions

	- นำข้อมูลจาก website หรือ api ผ่าน bs4 บน airflow
	- เลขล็อตเตอรี่รางวัลที่ 1 กับเลขท้าย 2, 3 ตัว ของแต่ละเดือน (เริ่มจาก มค. 64) หรือ
	- ข้อมูลคู่เหรียญในเว็บ binance หรือข้อมูลที่ นศ. สนใจ 

## ส่งเป็น MS Word, PDF

	# python file ของ airflow 
	# รายงาน
	        - รายละเอียดของที่มาของข้อมูล (ที่มา วันที่อับเดท ความถี่ในการอับเดท และข้อมูลอื่นๆ)
	        - รายละเอียดของข้อมูล (ชื่อข้อมูล ชื่อฟิล คอลัม ประเภทข้อมูล คำอธิบาย)
	        - ประโยชน์ที่ได้รับจากการรวบรวมข้อมูล (2-3 บรรทัด)

## เครื่องมือที่ใช้

	- Elastic Cloud Server (Huawei cloud)
	- Docker
	- Apache Airflow
	- MySQL

## คำสั่งเปิด mysql ถ้า docker

	sudo kill `sudo lsof -t -i:3306`

	docker ps
	docker exec -it airflow-airflow-worker-1 bash 
	pip install mysql-connector
	exit
	docker exec -it airflow-airflow-webserver-1 bash 
	pip install mysql-connector
	exit
	docker exec -it airflow-airflow-scheduler-1 bash 
	pip install mysql-connector
	exit
	docker-compose restart


