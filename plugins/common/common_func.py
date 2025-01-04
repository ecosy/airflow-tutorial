def get_sftp():
    print('=== sftp 작업을 시작합니다.===')

def regist(name, sex, *args):
    print(f'name : {name}')
    print(f'sex : {sex}')
    print(f'others : {args}')

def regist_with_kwargs(name, sex, *args, **kwargs):
    print(f'name : {name}')
    print(f'sex : {sex}')
    print(f'args : {args}')

    print(f'email : {kwargs.get("email")}')
    print(f'phone : {kwargs.get("phone")}')