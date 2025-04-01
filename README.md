# analytics_engineering_curse

O propósito deste repositório é armazenar e permitir através do github codespace a práticas de temas de analitycs enginering desenvolvidas no curso de analytics engineering desenvolvido para Ada tech.

Neste ambinete temos a estrutura necessaria para trabalhar com tratamento de dados com Python (folder pipeline), modelagem de dados com dbt core (folder datawarehouse), utilização do duckdb como datawarehouse (folder datawarehouse) e praticas de data quality com great expectation (folder data quality).

Para desenvolver o modelo dimensional e fixar os conhecimentos de analytics engineering, iremos utilizar um sample de dados de operações do tesour direto nacional (https://dados.gov.br/dados/busca?termo=tesouro).

Para utilização deste ambiente é necessrio inicialmente seguir os passos abaixo:

- Realizar a clonagem do repositorio para ambiente local;
- Realizar o push deste repositorio local, para um repositório criado pelo utilizador em sua conta do github;
- Criar uma instância do github codespace no repositório do github, na qual o repositório local foi enviado.
- realizar a instalação do duckdb atraves de do comando *curl https://install.duckdb.org | sh*;
- Ao acessar o Githib codespace executar o comando *pip install -r requirements.txt* para a instalação das bibliotecas necessárias;
- Executar os comando em python, para leitura do csv e import para o duckdb: *python prepate_investidors.py* e *python prepare_operations*
- Para acesso ao duckdb através da biblioteca harlequin (https://harlequin.sh/) executar o comando no terminal *harlequin curso/datawarehouse/tesouro.duckdb*;

## Usage examples

