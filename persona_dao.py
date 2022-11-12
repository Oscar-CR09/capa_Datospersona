import conexion
from conexion import Conexion
from cursor_del_pool import CursorDelPool
from persona import Persona
from logger_base import log


class PersonaDAO:
    """
    DAO (Data Acces Objeto)
    CRUD (Create - Read - Update - Delete)
    """
    _SELECIONAR = 'SELECT * FROM persona ORDER BY id_persona'
    _INSERT = 'INSERT INTO persona(nombre, apellido, email) VALUES(%s,%s,%s)'
    _ACTUALIZAR = 'UPDATE persona SET nombre=%s, apellido=%s,email=%s WHERE id_persona=%s'
    _ELIMINAR = 'DELETE FROM persona WHERE id_persona=%s'

    @classmethod
    def seleccionar(cls):

        with CursorDelPool() as cursor:
            cursor.execute(cls._SELECIONAR)
            registros = cursor.fetchall()
            personas = []
            for registro in registros:
                persona = Persona(registro[0], registro[1], registro[3])
                personas.append(persona)
            return personas

    @classmethod
    def insertar(cls, persona):
        with CursorDelPool() as cursor:
            valores = (persona.nombre, persona.apellido, persona.email)
            cursor.execute(cls._INSERT, valores)
            log.debug(f'Persona insertada: {persona}')
            return cursor.rowcount

    @classmethod
    def actualizar (cls,persona):
        with CursorDelPool() as cursor:
            valores = (persona.nombre, persona.apellido, persona.email, persona.id_persona)
            cursor.execute(cls._ACTUALIZAR, valores)
            log.debug(f'Personas Actualizada: {persona}')
            return cursor.rowcount

    @classmethod
    def eliminar (cls,persona):
        with CursorDelPool() as cursor:
            valores = (persona.id_persona,)
            cursor.execute(cls._ELIMINAR, valores)
            log.debug(f'Objeto eliminado: {persona}')
            return cursor.rowcount


if __name__ == '__main__':

    # insertar un registro
    persona1 = Persona(nombre='Alejandra', apellido='Telles', email='atelles@mail.com')
    personas_insertadas = PersonaDAO.insertar(persona1)
    log.debug(f'Personas insertadas: {personas_insertadas}')

    persona1 = Persona(1,'Juan ','Perez', 'jperez@mail.com')
    personas_actualizadas = PersonaDAO.actualizar(persona1)
    log.debug(f'Persona actualizadas: {personas_actualizadas}')

    # Eliminar registro

    persona1 = Persona(id_persona=12)
    persona_eliminadas = PersonaDAO.eliminar(persona1)
    log.debug(f'Cuantas personas fueron eliminadas: {persona_eliminadas} ')

    # selecionar objetos
    personas = PersonaDAO.seleccionar()
    for persona in personas:
        log.debug(persona)


'''
        with Conexion.obternerConexion() as conexion:
            with conexion.cursor() as cursor:
                cursor.execute(cls._SELECIONAR)
                registros = cursor.fetchall()
                personas = []
                for registro in registros:
                    persona = Persona(registro[0], registro[1], registro[3])
                    personas.append(persona)
                return personas
                
                
                @classmethod
    def insertar(cls, persona):
        with Conexion.obternerConexion():
            with Conexion.obtenerCursor() as cursor:
                valores = (persona.nombre, persona.apellido, persona.email)
                cursor.execute(cls._INSERT, valores)
                log.debug(f'Persona insertada: {persona}')
                return cursor.rowcount

    @classmethod
    def actualizar (cls,persona):
        with Conexion.obternerConexion():
            with Conexion.obtenerCursor() as cursor:
                valores = (persona.nombre, persona.apellido, persona.email, persona.id_persona)
                cursor.execute(cls._ACTUALIZAR, valores)
                log.debug(f'Personas Actualizada: {persona}')
                return cursor.rowcount

    @classmethod
    def eliminar (cls,persona):
        with Conexion.obternerConexion():
            with Conexion.obtenerCursor() as cursor:
                valores = (persona.id_persona,)
                cursor.execute(cls._ELIMINAR, valores)
                log.debug(f'Objeto eliminado: {persona}')
                return cursor.rowcount

'''