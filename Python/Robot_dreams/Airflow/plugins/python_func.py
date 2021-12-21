
def py_func(**kwargs):
    # **kwargs - get any function params
    print('Hello from inside Python function')
    print(f'Param: {kwargs["execution_date"]}')     # Airflow system param
    a = 10
    a = a * a * a
    print(a)
