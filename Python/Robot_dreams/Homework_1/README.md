###### **Домашнее задание 1**

Напишите Python программу, которая должна:
- считывать конфигурационный файл, в котором есть вся информация для подключения к API
- подключиться к API и выгрузить данные на диск. Данные должны быть партиционированы по директориям по дате

```json
API
url: https://robot-dreams-de-api.herokuapp.com
endpoint: /out_of_stock
payload: {"date": "2021-01-02"}
auth: JWT <jwt_token>
output type: JSON
expected output:
[
    {
        "product_id": 47066,
        "date": "2021-01-02"
    },
    {
        "product_id": 35855,
        "date": "2021-01-02"
    },
...
]

AUTH
endpoint: /auth
payload: {"username": "rd_dreams", "password": "djT6LasE"}
output type: JWT TOKEN
expected output:
{
    "access_token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE2MTc4ODMwODMsImlhdCI6MTYxNzg4Mjc4MywibmJmIjoxNjE3ODgyNzgzLCJpZGVudGl0eSi6MX0.-6m9MzmbBN1H9yGdtH799YZMiKuumgx-rwit_HllxyQ"
}
```