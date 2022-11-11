from logger_base import log
from conexion import Conexion


class CursorDelPool:
    def __init__(self):
        self._conexion = None
        self._cursor = None

    def __enter__(self):
        log.debug('Inicio del metodo with __enter__ ')
        self._conexion = Conexion.obternerConexion()
        self._cursor = self._conexion.cursor()
        return self._cursor

    def __exit__(self, tipo_excepcion, valor_exepcion, detalle_excepcion ):
        log.debug('Se ejecuta metodo __exit__ ')
        if valor_exepcion:
            self._conexion.rollback()
            log.error(f'Ocurrio una exepcion: {valor_exepcion} {tipo_excepcion} {detalle_excepcion} ')

        else:
            self._conexion.commit()
            log.debug('Commit de la transaccion ')

        self._cursor.close()
        Conexion.liberarConexion(self._conexion)
