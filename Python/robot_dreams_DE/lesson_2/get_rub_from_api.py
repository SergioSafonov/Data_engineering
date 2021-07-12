import json
import requests     # previously run in Terminal: >pip3.8 install requests

def app():

 #   url = "http://api.exchangeratesapi.io/v1/latest?access_key=a0ec6e79d368336768d78dd4bfc06e1f"
     url = "http://api.exchangeratesapi.io/v1/latest?access_key=a0ec6e79d368336768d78dd4bfc06e1f&symbols=RUB"
     response = requests.get(url)
     print(response.text)
     print(type(response.text))      # class str
     print(response.json())
     print(type(response.json))      # class method

     with open('./data/RUB.json', 'w') as f:
         data = response.json()
         rates = data['rates']
         json.dump(rates, f)
#        json.dump(response.json(), f)
         f.close()

# for direct call this function app()
if __name__ == '__main__':
    app()

