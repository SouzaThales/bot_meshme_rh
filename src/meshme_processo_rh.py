import sys
from datetime import datetime

import pandas as pd
import requests
from imap_tools import AND, MailBox
from validate_docbr import CPF, CNPJ

from utils import utils


class MeshmeProcessoRh():


    def __init__(self) -> None:
        self.utils = utils.Utils()


    def logar_email_imap(self, host:str, usuario:str, senha:str, pasta_inicial:str) -> object:
        try:   
            return MailBox(host).login(usuario, senha, pasta_inicial)
        except Exception as e:
            raise Exception (f'def logar_email_imap. Erro {str(e)}. Linha: {str(sys.exc_info()[-1].tb_lineno)}')
                    

    def achar_emails(self, caixa_email:object, assunto:str, data:datetime=datetime.now().date()) -> dict:
        try:
            lista_emails_encontrados = []
            # Filtra a caixa pelo assunto e a data
            for email in caixa_email.fetch(AND(subject=assunto, date=data)):
                lista_emails_encontrados.append(email)
            return lista_emails_encontrados
        except Exception as e:
            raise Exception (f'def achar_emails. Erro {str(e)}. Linha: {str(sys.exc_info()[-1].tb_lineno)}')


    def validar_anexo_email(self, email:object, nome_anexo:str) -> dict:
        try:
            lista_emails_filtrados = []
            email.reply_to_values
            # Se houver anexo
            if len(email.attachments) > 0:
                # Pra cada anexo, valida o nome e adiciona o email na lista de emails filtrados
                for anexo in email.attachments:
                    if nome_anexo == anexo.filename:
                        lista_emails_filtrados.append(email)
                        return True 
            return False
        except Exception as e:
            raise Exception (f'def validar_anexo_email. Erro {str(e)}. Linha: {str(sys.exc_info()[-1].tb_lineno)}')                


    def baixar_anexo_email(self, email:object, nome_anexo:str, caminho_excel:str) -> None:
        try:
            # Pra cada anexo no e-mail
            for anexo in email.attachments:
                # Valida o nome do arquivo
                if nome_anexo == anexo.filename:
                    # Escreve os bytes em um arquivo
                    with open(caminho_excel, 'wb') as arquivo_excel: 
                        arquivo_excel.write(anexo.payload)
                        return
        except Exception as e:
            raise Exception(f'função baixar_anexo_email. Erro {str(e)}. Linha: {str(sys.exc_info()[-1].tb_lineno)}')
                

    def validar_infos(self, json_infos) -> dict:
        try:
            cpf = CPF()
            cnpj = CNPJ()
            json_empresa = json_infos.get('empresa')
            json_funcionarios = json_infos.get('funcionarios')

            if json_empresa.get('cnpj') == '' or json_empresa.get('nome') == '' or not cnpj.validate(json_empresa.get('cnpj')):
                return False
                
            for funcionario in json_funcionarios:
                if funcionario.get('cpf') == '' or funcionario.get('nome_completo') == '' or funcionario.get('email_corporativo') == '' or not cpf.validate(funcionario.get('cpf')):
                    return False
            return True
        except Exception as e:
            raise Exception(f'def validar_infos. Msg erro: {str(e)} linha: {str(sys.exc_info()[-1].tb_lineno)}')


    def montar_json_infos(self, caminho_excel:str) -> list[dict]:
        try:
            list_temp_json_funcionario = []

            # Prepara os data-frames
            df_empresa = pd.read_excel(caminho_excel, sheet_name='Info_empresa')
            df_empresa['Data fim de parceria'] = pd.to_datetime(df_empresa["Data fim de parceria"]).dt.strftime('%Y-%m-%d').astype(str)
            df_empresa = df_empresa.fillna('')
            df_empresa = df_empresa.replace('NaT','')
            df_empresa = df_empresa.replace('nan','')

            df_infos_funcionarios = pd.read_excel(caminho_excel, sheet_name='Info_funcionarios')
            df_infos_funcionarios['Data de fim de contrato'] = pd.to_datetime(df_infos_funcionarios["Data de fim de contrato"]).dt.strftime('%Y-%m-%d').astype(str)
            df_infos_funcionarios['Data de admissão'] = pd.to_datetime(df_infos_funcionarios["Data de admissão"]).dt.strftime('%Y-%m-%d').astype(str)
            df_infos_funcionarios = df_infos_funcionarios.fillna('')
            df_infos_funcionarios = df_infos_funcionarios.replace('NaT','')
            df_infos_funcionarios = df_infos_funcionarios.replace('nan','')

            # Monta o json principal com as infos da empresa
            json_info = {'empresa':{
                        'cnpj':df_empresa['ID (CNPJ)*'].values[0],
                        'nome':df_empresa['Nome da Empresa*'].values[0],
                        'email':df_empresa['E-mail do contato da empresa'].values[0],
                        'telefone':df_empresa['Telefone'].values[0],
                        'endereco':df_empresa['Endereço do escritório'].values[0],
                        'data_fim_parceria':df_empresa['Data fim de parceria'].values[0]
                        },
            }

            # Pra cada funcionario no frame
            for index , row in df_infos_funcionarios.iterrows():
                # Insere o json com as infos do funcionario na lista
                list_temp_json_funcionario.append(
                    {
                        'cpf':row['ID (CPF)*'],
                        'nome_completo': row['Nome completo*'],
                        'email_corporativo': row['E-mail corporativo*'],
                        'telefone':row['Telefone'],
                        'data_admissao':row['Data de admissão'],
                        'endereco_escritorio': row['Endereço do escritório base'],
                        'data_fim_contrato':row['Data de fim de contrato']
                    } 
                        )

            # Atualiza o json principal inserindo a lista de funcionarios e retorna
            json_info.update({'funcionarios':list_temp_json_funcionario})

            return json_info
        except Exception as e:
            raise Exception(f'def montar_json_infos. Msg erro: {str(e)} linha: {str(sys.exc_info()[-1].tb_lineno)}')
                

    def deletar_email(self, caixa_email:object, email_processado:object) -> None:
        try:
            caixa_email.delete(email_processado.uid)  
        except Exception as e:
            raise Exception(f'def deletar_email. Msg erro: {str(e)} linha: {str(sys.exc_info()[-1].tb_lineno)}')


    def integracao_api(self, json_infos_planilha:dict, params_api:dict, logger:object) -> None:
        try:
            falhas = 0
            desativar_tudo = False
            data_hoje_insert_banco = datetime.now().strftime('%Y-%m-%d')    
            json_empresa = json_infos_planilha.get('empresa')
            cnpj_sem_mascara = self.utils.remover_mascara(json_empresa.get('cnpj'))
            url_default = params_api.get('url_default')
            url_get_empresa  = f'{url_default}/api/v1/profile/company/{cnpj_sem_mascara}'
            url_post_empresa = f'{url_default}/api/v1/profile/company/{cnpj_sem_mascara}'
            url_put_empresa = f'{url_default}/api/v1/profile/company/' 
            url_get_funcionario = f'{url_default}/api/v1/profile/employee/'
            url_post_funcionario = f'{url_default}/api/v1/profile/employee/'
            url_put_funcionario = f'{url_default}/api/v1/profile/employee/'
            url_put_desativar_funcionarios_company = f'{url_default}/api/v1/profile/company/end-partnership/'
            url_put_desativar_login = f'{url_default}/api/v1/profile/login/deactivation/'

            logger.debug(f"\tEmpresa {json_empresa.get('cnpj')}")

            # Se a empresa não estiver cadastrada
            retorno_api = requests.get(url_get_empresa)
            if retorno_api.status_code == 404 and 'USUARIO NÃO ENCONTRADO' in retorno_api.text.upper():
                json_post = {
                            "nmCompanyName":json_empresa.get('nome'),
                            "nmAddress":"Principal",
                            "dsAddress":"Praça da Sé" if json_empresa.get('endereco') == '' else json_empresa.get('endereco'),
                            "dsCountry":"Brasil",
                            "dsState":"São Paulo",
                            "dsCity":"São Paulo",
                            "cdPostalCode":"01001-901",
                            "flActiveAdress":1,
                            "flMainAdress":1,
                            "dsEmail":'empresa@empresa.com' if json_empresa.get('email') == '' else json_empresa.get('email'),
                            "flActiveEmail":1,
                            "flMainMail":1,
                            "nmPhone":"Comercial",
                            "nrPhone":"11 9999-9999" if json_empresa.get('telefone') == '' else str(json_empresa.get('telefone')).replace(' ','').replace('.0',''),
                            "flActivePhone":1,
                            "flMainPhone":1,
                            }
                retorno_api = requests.post(url_post_empresa, json=json_post)
                if retorno_api.status_code != 200:
                    logger.debug(f'\t\t\tPOST cadastrar empresa = [{retorno_api.status_code}] {retorno_api.reason}')
                    falhas = falhas + 1
                else:
                    logger.debug('\t\t\tPOST cadastrar empresa = [200] Sucesso')

            # Se a empresa estiver cadastrada e houver fim de parceria
            elif retorno_api.status_code == 200 and json_empresa.get('data_fim_parceria') != '':
                desativar_tudo = True
                id_company = retorno_api.json()[0].get('idCompany')
                json_put = {
                            'flCompanyActivePartnership':0,
                            'dtCompanyEndPartnership': json_empresa.get('data_fim_parceria'),
                            # 'dtCompanyLastUpdate': data_hoje_insert_banco
                            }
                retorno_api = requests.put(f'{url_put_empresa}{id_company}', json=json_put)
                if retorno_api.status_code != 200:
                    logger.debug(f'\t\t\tPUT desativar empresa = [{retorno_api.status_code}] {retorno_api.reason}')
                    falhas = falhas + 1
                else:
                    logger.debug('\t\t\tPUT desativar empresa = [200] Sucesso')
                
            # Se a empresa estiver cadastrada e não houver data de fim de parceria
            elif retorno_api.status_code == 200:
                id_company = retorno_api.json()[0].get('idCompany')
                json_put = {
                            'flCompanyActivePartnership':1,
                            'dtCompanyEndPartnership': '',
                            # 'dtCompanyLastUpdate': data_hoje_insert_banco
                            }
                retorno_api = requests.put(f'{url_put_empresa}{id_company}', json=json_put)
                if retorno_api.status_code != 200:
                    logger.debug(f'\t\t\tPUT ativar empresa = [{retorno_api.status_code}] {retorno_api.reason}')
                    falhas = falhas + 1
                else:
                    logger.debug('\t\t\tPUT ativar empresa = [200] Sucesso')
            
            # Se for outro cenario
            else:
                texto_erro = retorno_api.text[retorno_api.text.find('<title>')+7:retorno_api.text.find('</title>')]
                logger.debug(f'\t\tGet empresa = [{retorno_api.status_code}] {retorno_api.reason} Erro: {texto_erro}')
                falhas = falhas + 1

            # Se não houve nenhum erro em relação ao cadastro da empresa
            if falhas == 0:
                id_company = requests.get(url_get_empresa).json()[0].get('idCompany')
                # Se for para desativar tudo
                if desativar_tudo:
                    retorno_api = requests.put(f'{url_put_desativar_funcionarios_company}{id_company}', json={})
                    if retorno_api.status_code != 200:
                        logger.debug(f'\t\t\tPUT desativar usuarios = [{retorno_api.status_code}] {retorno_api.reason}')
                        falhas = falhas + 1
                    else:
                        logger.debug('\t\t\tPUT desativar usuarios = [200] Sucesso') 
                else:
                    # Pra cada funcionário no json
                    for funcionario in json_infos_planilha.get('funcionarios'):
                        try:      
                            cpf_sem_mascara = self.utils.remover_mascara(funcionario.get('cpf'))
                            logger.debug(f"\t\t\tFuncionario {funcionario.get('cpf')}")

                            # Se não estiver cadastrado
                            retorno_api = requests.get(f'{url_get_funcionario}{cpf_sem_mascara}/{id_company}')
                            if retorno_api.status_code == 404 and 'FUNCIONÁRIO NÃO ENCONTRADO' in retorno_api.text.upper() :
                                json_post = {
                                            "nmFirstNames":funcionario.get('nome_completo').split(' ')[0],
                                            "nmFamilyNames":" " .join(funcionario.get('nome_completo').split(' ')[1:]),
                                            "dsRole":"",
                                            "dsProfile":"",
                                            "dsUrlLinkedin":"",
                                            "nmAddress":"Principal",
                                            "dsAddress":"Praça da Sé",
                                            "dsCountry":"Brasil",
                                            "dsState":"São Paulo",
                                            "dsCity":"SãoPaulo",
                                            "cdPostalCode":"01001-901",
                                            "flActiveAdress":"1",
                                            "flMainAdress":"1",
                                            "nmPhone":"Comercial",
                                            "nrPhone":"11 9999-9999" if funcionario.get('telefone') == '' else '+55'+str(funcionario.get('telefone')).replace(' ','').replace('.0',''),
                                            "flActivePhone":1,
                                            "flMainPhone":1,
                                            "dsEmail":'empresa@empresa.com' if funcionario.get('email_corporativo') == '' else funcionario.get('email_corporativo'),
                                            "flActiveEmail":1,
                                            "flMainMail":1
                                            }
                                    
                                retorno_api = requests.post(f'{url_post_funcionario}{cpf_sem_mascara}/{id_company}', json=json_post)
                                if retorno_api.status_code != 200:
                                    logger.debug(f'\t\t\t\tPOST cadastrar usuario = [{retorno_api.status_code}] {retorno_api.reason}')
                                    falhas = falhas + 1
                                else:
                                    logger.debug('\t\t\t\tPOST cadastrar usuario = [200] Sucesso')

                            # Se o funcionario estiver cadastrado e houver fim de contrato
                            elif retorno_api.status_code == 200 and funcionario.get('data_fim_contrato') != '': 
                                id_user = retorno_api.json().get('idUser')    
                                json_put = {
                                            "user":
                                                {
                                                    "dtUserEndContract": funcionario.get('data_fim_contrato'),
                                                    "dtLayoff": funcionario.get('data_fim_contrato'),
                                                    "flUserActiveContract": "0",
                                                },
                                            "address":{},
                                            "email":{},
                                            "phone":{}
                                            }
                                retorno_api = requests.put(f'{url_put_funcionario}{id_user}', json=json_put)
                                if retorno_api.status_code != 200:
                                    logger.debug(f'\t\t\t\tPUT desativar usuario = [{retorno_api.status_code}] {retorno_api.reason}')
                                    falhas = falhas + 1
                                else:
                                    logger.debug('\t\t\t\tPUT desativar usuario = [200] Sucesso')

                                # Desativa o login do usuario
                                retorno_api = requests.put(f'{url_put_desativar_login}{id_user}', json={})
                                if retorno_api.status_code != 200:
                                    logger.debug(f'\t\t\t\tPUT desativar login = [{retorno_api.status_code}] {retorno_api.reason}')
                                    falhas = falhas + 1
                                else:
                                    logger.debug('\t\t\t\tPUT desativar login = [200] Sucesso')
                                
                            # Se o funcionario estiver cadastrado e não houver fim de contrato
                            elif retorno_api.status_code == 200:
                                id_user = retorno_api.json().get('idUser')
                                json_put = {
                                            "user":
                                                {
                                                    "flUserActiveContract": "1",
                                                    "dtUserEndContract": '',
                                                    "dtLayoff": '',
                                                },
                                            "address":{},
                                            "email":{},
                                            "phone":{}
                                            }
                                retorno_api = requests.put(f'{url_put_funcionario}{id_user}', json=json_put)
                                if retorno_api.status_code != 200:
                                    logger.debug(f'\t\t\t\tPUT ativar usuario = [{retorno_api.status_code}] {retorno_api.reason}')
                                    falhas = falhas + 1
                                else:
                                    logger.debug('\t\t\t\tPUT ativar usuario = [200] Sucesso')
                           
                            # Se for outro cenario
                            else:
                                texto_erro = retorno_api.text[retorno_api.text.find('<title>')+7:retorno_api.text.find('</title>')]
                                logger.debug(f'\t\t\tGet funcionario = [{retorno_api.status_code}] {retorno_api.reason} Erro: {texto_erro}')
                                falhas = falhas + 1

                        except Exception as e:
                            logger.debug(f"\t\t\t\tMsg erro: {str(e)} linha: {str(sys.exc_info()[-1].tb_lineno)}")
                            falhas += 1
            
            if falhas != 0:
                return False
            else:
                return True
        except Exception as e:
            raise Exception(f'def integracao_api. Msg erro: {str(e)} linha: {str(sys.exc_info()[-1].tb_lineno)}')