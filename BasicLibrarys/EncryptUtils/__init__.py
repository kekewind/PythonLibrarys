from BasicLibrarys.EncryptUtils.AESRijndael import AesRijndael
from BasicLibrarys.EncryptUtils.BFEncrypt import BfEncrypt
from BasicLibrarys.EncryptUtils.RSAEncrypt import RsaEncrypt
from BasicLibrarys.EncryptUtils.IDEAEncrypt import IdeaEncrypt
from BasicLibrarys.EncryptUtils.CAST5Encrypt import Cast5Encrypt


class EncryptUtils:
    def __init__(self):
        self._aes = AesRijndael()
        self._rsa = RsaEncrypt()
        self._blow_fish = BfEncrypt()
        self._idea = IdeaEncrypt()
        self._cast = Cast5Encrypt()

    def rsa_new_key(self, pubkey_path=None, prikey_path=None, pri_password=None):
        self._rsa.new(pubkey_path, prikey_path, pri_password)
        print("RSA Key Generate Done!")

    def rsa_sign(self, message, pri_password=None):
        return self._rsa.sign(message, pri_password)

    def rsa_verify(self, message, signature, pri_password=None):
        return self._rsa.verify(message, signature, pri_password)

    def encrypt_byte(self, input_str, password, algorithm):
        if algorithm == "rsa":
            return self._rsa.rsa_byte_encrypt(input_str, password)
        elif algorithm == "aes":
            return self._aes.aes_encrypt(input_str, password)
        elif algorithm == "idea":
            return self._idea.idea_byte_encrypt(input_str, password)
        elif algorithm == "blowfish":
            return self._blow_fish.bf_encrypt(input_str, password)
        elif algorithm == "cast5":
            return self._cast.cast_byte_encrypt(input_str, password)

    def decrypt_byte(self, input_str, password, algorithm):
        if algorithm == "rsa":
            return self._rsa.rsa_byte_decrypt(input_str, password)
        elif algorithm == "aes":
            return self._aes.aes_decrypt(input_str, password)
        elif algorithm == "idea":
            return self._idea.idea_byte_decrypt(input_str, password)
        elif algorithm == "blowfish":
            return self._blow_fish.bf_decrypt(input_str, password)
        elif algorithm == "cast5":
            return self._cast.cast_byte_decrypt(input_str, password)

    def encrypt_str(self, input_str, password, algorithm):
        if algorithm == "rsa":
            return self._rsa.rsa_str_encrypt(input_str, password)
        elif algorithm == "aes":
            return self._aes.aes_str_encrypt(input_str, password)
        elif algorithm == "idea":
            return self._idea.idea_str_encrypt(input_str, password)
        elif algorithm == "blowfish":
            return self._blow_fish.bf_str_encrypt(input_str, password)
        elif algorithm == "cast5":
            return self._cast.cast_str_encrypt(input_str, password)

    def decrypt_str(self, input_str, password, algorithm):
        if algorithm == "rsa":
            return self._rsa.rsa_str_decrypt(input_str, password)
        elif algorithm == "aes":
            return self._aes.aes_str_decrypt(input_str, password)
        elif algorithm == "idea":
            return self._idea.idea_str_decrypt(input_str, password)
        elif algorithm == "blowfish":
            return self._blow_fish.bf_str_decrypt(input_str, password)
        elif algorithm == "cast5":
            return self._cast.cast_str_decrypt(input_str, password)

    def encrypt_file(self, file_path, password, algorithm, encrypt_file=None):
        if algorithm == "rsa":
            return self._rsa.rsa_file_encrypt(file_path, password, encrypt_file)
        elif algorithm == "aes":
            return self._aes.aes_file_encrypt(file_path, password, encrypt_file)
        elif algorithm == "idea":
            return self._idea.idea_file_encrypt(file_path, password, encrypt_file)
        elif algorithm == "blowfish":
            return self._blow_fish.bf_file_encrypt(file_path, password, encrypt_file)
        elif algorithm == "cast5":
            return self._cast.cast_file_encrypt(file_path, password, encrypt_file)

    def decrypt_file(self, encrypt_file, password, algorithm, decrypt_file):
        if algorithm == "rsa":
            return self._rsa.rsa_file_decrypt(encrypt_file, password, decrypt_file)
        elif algorithm == "aes":
            return self._aes.aes_file_decrypt(encrypt_file, password, decrypt_file)
        elif algorithm == "idea":
            return self._idea.idea_file_decrypt(encrypt_file, password, decrypt_file)
        elif algorithm == "blowfish":
            return self._blow_fish.bf_file_decrypt(encrypt_file, password, decrypt_file)
        elif algorithm == "cast5":
            return self._cast.cast_file_decrypt(encrypt_file, password, decrypt_file)
