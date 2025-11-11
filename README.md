# dashboarder
Project for my home IoT infrastructure. 

---

This is learning project, using RUST i am trying to create home dashboard
and all needed sub functions. All will run in docker compose for now, 
in future it should be deployable into kubernetes.

---

This project is from multiple components:

- input parsers - input parser which read data from specified input,
parse them to "normalized" format and save them properly :
    - mqtt input
    - read multiple DS18B20 over 1-wire_to_i2c convertor
    - scrap some interesting informations from specified websites
    - access some public apis and store informations from them properly
- save services - multiple services which access normalized data in queue 
and store them in propriet storage we choose
- business logic which reads data, in need work with them and store them to another location
- web server which allows working with data via rest api
- front end web server which provides dashboard and communicate with backend api server

---

For now we deploy needed databases and other services via docker compose.

---

