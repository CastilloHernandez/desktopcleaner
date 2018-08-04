import sqlite3
import os
import hashlib
import time
import shutil

def hashArchivo(fname):
	hash_md5 = hashlib.md5()
	try:
		with open(fname, "rb") as f:
			for chunk in iter(lambda: f.read(4096), b""):
				hash_md5.update(chunk)
	except:
		print 'error accediendo al archivo ' + fname
	return hash_md5.hexdigest()

def removeIfEmpty(dir, raiz=0):
	for entry in os.listdir(dir):
		#print "entry "+ os.path.join(dir, entry) +" "+ str(os.path.isdir(os.path.join(dir, entry)))
		if os.path.isdir(os.path.join(dir, entry)):
			removeIfEmpty(os.path.join(dir, entry))
	if raiz == 0:
		if not os.listdir(dir):
			print 'borrando directorio vacio '+ dir
			os.rmdir(dir)		


def crearDirectorios(ruta):
	if not os.path.isdir(ruta):
		print 'crear directorio '+ ruta 
		os.makedirs(ruta)

			
def removeLost():
	global db
	cursor = db.cursor()
	cursor.execute('SELECT path FROM files WHERE lastcheck < '+ str(time.time() - 604800))
	for row in cursor.fetchall():
		print 'archivo perido ' + row[0]
	cursor.execute('DELETE FROM files WHERE lastcheck < '+ str(time.time()  - 604800))
	db.commit()
	
	
def buscarArchivos(dir):
	global db
	cursor = db.cursor()
	for root, dirs, files in os.walk(dir):
		print 'buscando en '+ root
		#if not os.path.normpath(root) == os.path.normpath(dir):
		#	removeIfEmpty(root)
		for file in files:
			archivoEncontrado = 0
			archivoModificado = 0
			fechaModificado = 0
			idActual = 0
			rutaActual = str(os.path.join(root, file))
			if file == 'desktop.ini':
				print 'archivo omitido ' + rutaActual
			else:
				try:
					tamanioActual = os.path.getsize(rutaActual)
				except:
					print 'error al acceder ' + rutaActual
					continue
				hashActual = hashArchivo(rutaActual)
				cursor.execute('SELECT id, path, hash, lastdiff FROM files WHERE path="'+rutaActual+'"')
				for row in cursor.fetchall():
					archivoEncontrado = 1
					idActual = row[0]
					fechaModificado = row[3]
					if not row[2] == hashActual:
						archivoModificado = 1
				if archivoEncontrado:
					if archivoModificado:
						print 'archivo modificado '+ rutaActual
						cursor.execute('UPDATE files SET size=?, hash=?, lastcheck=?, lastdiff=? WHERE id=?', (tamanioActual, hashActual, time.time(), time.time(), idActual))
						db.commit()
					else:
						if time.time() - fechaModificado > 604800:
							print 'mover archivo '+ rutaActual
							dirDestino = os.path.normpath(str(root).replace(dir, 'C:\\Temp\\'))
							crearDirectorios(dirDestino)
							shutil.move(rutaActual, os.path.join(dirDestino, file))
							if os.path.isfile(os.path.join(dirDestino, file)):
								#if not os.path.normpath(root) == os.path.normpath(dir):
								#	removeIfEmpty(root)
								cursor.execute('DELETE FROM files WHERE id='+ str(idActual))
								db.commit()		
						else:
							cursor.execute('UPDATE files SET lastcheck=? WHERE id=?', (time.time(), idActual))
							db.commit()
				else:
					print 'nuevo archivo '+ rutaActual
					cursor.execute('INSERT INTO files(path, name, size, hash, lastcheck, lastdiff) VALUES(?,?,?,?,?,?)', (rutaActual, file, tamanioActual, hashActual, time.time(), time.time() ))
					db.commit()
	removeIfEmpty(dir, 1)
					
db = sqlite3.connect('desktopcleaner.db')
db.text_factory = lambda x: unicode(x, "utf-8", "ignore")

cursor = db.cursor()
cursor.execute('CREATE TABLE IF NOT EXISTS files (id INTEGER PRIMARY KEY, path TEXT, name TEXT, size INTEGER, hash TEXT, lastcheck INTEGER, lastdiff INTEGER)')
db.commit()
buscarArchivos('C:\\Users\\Emmanuel Castillo\\Documents\\Bluetooth Folder\\')
buscarArchivos('C:\\Users\\Emmanuel Castillo\\Dropbox\\Capturas de pantalla\\')
buscarArchivos('C:\\Users\\Emmanuel Castillo\\Desktop\\')
removeLost()
db.close()
