import click


@click.command()
@click.argument('name', default='guest')
@click.option("--surname", prompt="Your surname", help="Provide your surname")
@click.argument('age', type=int)
def hello(name, surname, age):
    click.echo(f'Hello {name} {surname}. He is {age} years old')


if __name__ == '__main__':
    hello()

