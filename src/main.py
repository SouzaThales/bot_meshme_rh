from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import logging
import pathlib
import smtplib
import sys

import yaml

import meshme_processo_rh
from utils import crypt_aes

log_file = '.\\temp\\log.log'

# Apaga o arquivo log
file = pathlib.Path(log_file)
if file.exists():
    file.unlink()

logger = logging.getLogger()
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

# Define basic configuration
logging.basicConfig(
    # Define logging level
    level=logging.DEBUG,
    # Declare the object we created to format the log messages
    format='[%(asctime)s] %(message)s',
    # Declare handlers
    handlers=[
        # Log file and console log
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout),
    ]
)


def montar_email_fim_processamento(params:dict, corpo:str) -> object:
    try:
        msg = MIMEMultipart()

        msg['Subject'] = 'RPA - Processamento'
        msg['From'] = params.get('usuario')
        msg['To'] = params.get('email_adm_meshme')
        msg.attach(MIMEText(corpo, 'plain'))

        attachment = open(log_file,'rb')
        part = MIMEBase('application', 'octet-stream')
        part.set_payload((attachment).read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', "attachment; filename= %s" % 'log.log')

        msg.attach(part)
        attachment.close()
       
        return msg
    except Exception as e:
        raise Exception(f'def montar_email_fim_processamento. Msg erro: {str(e)} linha: {str(sys.exc_info()[-1].tb_lineno)}')


def montar_email_retorno_empresa(email_object:object, params:object, corpo:str) -> object:
        try:
            msg = MIMEMultipart()
            msg['Subject'] = 'RPA - Processamento'
            msg['From'] = params.get('usuario')
            msg['To'] = email_object.from_
            msg['CC'] = ','.join(email_object.cc)
            msg.attach(MIMEText(corpo, 'plain'))
            return msg
        except Exception as e:
            raise Exception(f'def montar_email_retorno_empresa. Msg erro: {str(e)} linha: {str(sys.exc_info()[-1].tb_lineno)}')


if __name__ == "__main__":
    try:
        logger.debug(f'Processo iniciado')
        
        # Pega os parametros
        with open('.\\config\\config.yaml') as file:
            params = yaml.load(file, Loader=yaml.FullLoader)
            params_imap = params.get('imap')
            params_smtp = params.get('smtp')
            params_api = params.get('api')
        logger.debug(f'Parametros capturados')

        # Seta algumas variaveis q serão utilizadas
        nome_anexo = params_imap.get('nome_anexo')
        caminho_excel = f".\\temp\\{nome_anexo}"
        nenhuma_falha = False
        
        bot = meshme_processo_rh.MeshmeProcessoRh()

        # Conectar na caixa de email via IMAP
        senha_uncrypt_imap = crypt_aes.AESCipher().decrypt(params_imap.get('key'), params_imap.get('senha'))
        caixa_email_imap = bot.logar_email_imap(params_imap.get('host'), params_imap.get('usuario'), senha_uncrypt_imap, params_imap.get('pasta_inicial_email'))
        logger.debug('Login no e-mail realizado com sucesso')
        senha_uncrypt_imap = None
        # Faz a busca pelos filtros de assunto e data
        lista_emails_encontrados = bot.achar_emails(caixa_email_imap, params_imap.get('assunto_filtro'))

        # Conecta ao smpt para o envio dos emails 
        senha_uncrypt_smpt = crypt_aes.AESCipher().decrypt(params_smtp.get('key'), params_smtp.get('senha'))
        smtp = smtplib.SMTP(params_smtp.get('host'))
        smtp.starttls()
        smtp.login(params_smtp.get('usuario'), senha_uncrypt_smpt)
        senha_uncrypt_smpt = None

        # Se a lista estiver vazia
        if not len(lista_emails_encontrados):
            logger.debug('Nada para processar')
            msg = montar_email_fim_processamento(params_smtp, f"Nada para processar")
            smtp.sendmail(msg['From'],msg['To'],msg.as_string().encode('utf-8'))
            logger.debug('E-mail enviado')            
        else:
            logger.debug(f'{len(lista_emails_encontrados)} e-mail(s) encontrado(s)')

            # Percorre os emails
            for email in lista_emails_encontrados:
                try:
                    logger.debug(f'Inicio Processamento e-mail ({email.from_})')

                    # Valida o anexo do email
                    if not bot.validar_anexo_email(email, nome_anexo):
                        logger.debug(f"\tNenhum anexo com o nome padrão ({nome_anexo}) encontrado")
                        msg = montar_email_retorno_empresa(email, params_smtp, f"Nenhum anexo com o nome padrão ({nome_anexo}) encontrado")
                        smtp.sendmail(msg['From'],msg['To'],msg.as_string().encode('utf-8'))
                        logger.debug(f"\tE-mail de aviso enviado para a empresa")
                        bot.deletar_email(caixa_email_imap, email)
                        logger.debug(f"\tE-mail processado deletado")
                        continue
                    
                    # Baixa o anexo do email
                    bot.baixar_anexo_email(email, nome_anexo, caminho_excel)
                    logger.debug(f"\tAnexo Baixado")

                    # Monta o json com as infos
                    json_infos_planilha = bot.montar_json_infos(caminho_excel)
                    logger.debug(f"\tJSON com as infos da planilha montado")

                    # Valida se as infos estão ok
                    if not bot.validar_infos(json_infos_planilha):
                        logger.debug(f'Uma ou mais informações incorretas na planilha')
                        msg = montar_email_retorno_empresa(email, params_smtp, f'Uma ou mais informações incorretas na planilha, por favor verificar os campos obrigatórios')
                        smtp.sendmail(msg['From'],msg['To'],msg.as_string().encode('utf-8'))
                        logger.debug(f"\tE-mail de aviso enviado para a empresa")
                        bot.deletar_email(caixa_email_imap, email)
                        logger.debug(f"\tE-mail processado deletado")
                        continue

                    logger.debug(f"\tInfos do JSON validadas")

                    # Faz a integração com a api
                    if bot.integracao_api(json_infos_planilha, params_api, logger):
                        bot.deletar_email(caixa_email_imap, email)
                        logger.debug(f"\tE-mail processado deletado")
                        nenhuma_falha = True
                        
                except Exception as e:
                    nenhuma_falha = False
                    logger.debug(f"\tMsg erro: {str(e)} linha: {str(sys.exc_info()[-1].tb_lineno)}")

            # Envia o email informando o texto retornado da lib principal
            msg = montar_email_fim_processamento(params_smtp, 'Processo finalizado com sucesso' if nenhuma_falha else 'Processo finalizado com falha, por favor analisar o arquivo log em anexo')
            smtp.sendmail(msg['From'],msg['To'],msg.as_string().encode('utf-8'))
            logger.debug('E-mail report enviado para a adm do meshme')
    except Exception as e:
        raise Exception(f'Main. Msg erro: {str(e)} linha: {str(sys.exc_info()[-1].tb_lineno)}')