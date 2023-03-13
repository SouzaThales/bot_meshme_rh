import base64
import hashlib
import sys
from Crypto import Random
from Crypto.Cipher import AES

class AESCipher(object):

    def __init__(self): 
        self.bs = AES.block_size

    def encrypt(self, key, raw):
        try:
            key = hashlib.sha256(key.encode()).digest()
            raw = self._pad(raw)
            iv = Random.new().read(AES.block_size)
            cipher = AES.new(key, AES.MODE_CBC, iv)
            return base64.b64encode(iv + cipher.encrypt(raw.encode()))
        except Exception as e:
            raise Exception(f'Erro encrypt. Msg: {str(e)} linha: {str(sys.exc_info()[-1].tb_lineno)}')

    def decrypt(self, key, enc):
        try:
            key = hashlib.sha256(key.encode()).digest()
            enc = base64.b64decode(enc)
            iv = enc[:AES.block_size]
            cipher = AES.new(key, AES.MODE_CBC, iv)
            return self._unpad(cipher.decrypt(enc[AES.block_size:])).decode('utf-8')
        except Exception as e:
            raise Exception(f'Erro decrypt. Msg: {str(e)} linha: {str(sys.exc_info()[-1].tb_lineno)}')

    def _pad(self, s):
        return s + (self.bs - len(s) % self.bs) * chr(self.bs - len(s) % self.bs)

    @staticmethod
    def _unpad(s):
        return s[:-ord(s[-1:])]