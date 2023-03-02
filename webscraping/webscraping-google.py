import time

import pandas as pd

from bs4 import BeautifulSoup

from selenium import webdriver
import time

from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.options import Options


#empresas_receita = pd.read_csv("C://projetos/mentors/lead-generation/LG-poc/data/empresas_receita_streamlit.csv", dtype = "str")

empresas_receita = pd.read_csv("../data/empresas_receita_streamlit.csv", dtype = "str")


empresas_receita_unico = empresas_receita.drop_duplicates(subset=["CNPJ"])
empresas_receita_unico.reset_index(drop=["index"], inplace=True)

empresas_receita_unico["NOMES_BUSCA"] = empresas_receita_unico["NOME FANTASIA"] + ", " + empresas_receita_unico["MUNICIPIO"] + " - " + empresas_receita_unico["UF"]

dic_informacoes_google = {}
i = 0

for nome in empresas_receita_unico["NOMES_BUSCA"]:
    nome_empresa = nome.split(",")[0]
    dic_informacoes_google[nome_empresa] = {"tel": [], "link":[], "descricao":[]}

    option = Options()

    option.headless = False

    driver = webdriver.Firefox(options = option)
    url = "https://www.google.com"

    driver.get(url)
    driver.find_element("xpath", "/html/body/div[1]/div[3]/form/div[1]/div[1]/div[1]/div/div[2]/input").send_keys(nome)
    driver.find_element("xpath", "/html/body/div[1]/div[3]/form/div[1]/div[1]/div[1]/div/div[2]/input").send_keys(Keys.ENTER)

    time.sleep(5)

    print(f"buscando empresa {i}")
    i += 1

    quadro_principal = driver.find_element("xpath", '//*[@id="rso"]')
    html_content = quadro_principal.get_attribute('outerHTML')

    soup = BeautifulSoup(html_content, 'html.parser')
    
    for j in soup.find_all("div", attrs = {"class":"MjjYud"})[:-1]:
        try:
            link = j.find("a").get("href")
            dic_informacoes_google[nome_empresa]["link"].append(link)
            descricao = j.find("div", attrs={"class":"VwiC3b yXK7lf MUxGbd yDYNvb lyLwlc lEBKkf"}).text
            dic_informacoes_google[nome_empresa]["descricao"].append(descricao)
        
        except:
            pass
        
    try:
        quadro_lateral = driver.find_element("xpath", '//*[@id="rhs"]')
        print("loading...{}".format(nome_empresa))
        
        html_content = quadro_lateral.get_attribute('outerHTML')
        soup = BeautifulSoup(html_content, 'html.parser')
        dic_informacoes_google[nome_empresa]["tel"] = (soup.find_all("span", attrs = {"class":"LrzXr zdqRlf kno-fv"}))[0].text
        print("FONE OK!!!")

    except:
        dic_informacoes_google[nome_empresa]["tel"] = "NAO DISPONIBILIZADO"
        print("NO FONE...{}".format(nome_empresa))

    driver.quit()

df_informacoes_google = pd.DataFrame.from_dict(dic_informacoes_google, orient="index")

# df_informacoes_google.to_csv("C://projetos/mentors/lead-generation/LG-poc/data/df_informacoes_google.csv")

df_informacoes_google.to_csv("df_informacoes_google.csv")

