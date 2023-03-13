import sys

class Utils:


    def remover_mascara(self, documento):
        try:
            return documento.replace('.','').replace('-','').replace('/','')
        except Exception as e:
            raise Exception(f'Erro função remover_mascara: {str(e)} linha: {str(sys.exc_info()[-1].tb_lineno)}')


    def adicionar_mascara_cnpj(self, doc):
        try:
            doc = doc.strip()
            doc = ''.join([char for char in doc if char.isdigit()])
            doc = doc.zfill(14)

            new_doc = ''
            for i in range(len(doc)):
                if i in [2, 5]:
                    new_doc += '.'
                elif i == 8:
                    new_doc += '/'
                elif i == 12:
                    new_doc += '-'
                new_doc += doc[i]
            return new_doc
        except Exception as e:
            raise Exception(f'Erro função adicionar_mascara: {str(e)} linha: {str(sys.exc_info()[-1].tb_lineno)}')