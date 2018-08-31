package filestore

import (
	"bufio"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"hash/crc32"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

// Store описывает хранилище файлов.
type Store struct {
	root string
}

// Open открывает и возвращает хранилище файлов.
func Open(root string) (*Store, error) {
	// создаем каталог, если он еще не создан
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	return &Store{root: root}, nil
}

// FileInfo описывает информацию о сохраненном файле.
type FileInfo struct {
	Name     string `json:"location"`
	Mimetype string `json:"mimetype"`
	Size     int64  `json:"size"`
	CRC32    uint32 `json:"crc32"`
	MD5      string `json:"md5"`
}

// Create сохраняет файл в хранилище. В качестве имени файла используется
// комбинация из двух хешей. Файл сохраняется в подкаталоге prefix, если он
// задан, но данный prefix не учитывается в возвращаемой информации в имени
// файла.
func (s *Store) Create(prefix string, r io.Reader) (*FileInfo, error) {
	var root = filepath.Join(s.root, prefix) // добавляем префикс к корню
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	// создаем временный файл в корневом каталоге
	tmpfile, err := ioutil.TempFile(root, "~tmp")
	if err != nil {
		err.(*os.PathError).Path = tmpfileName // подменяем имя файла
		return nil, err
	}
	// в любом случае временный файл должен быть удален, если он не был
	// переименован, т.е. на момент окончания функции существует под временным
	// именем
	defer os.Remove(tmpfile.Name())

	// копируем содержимое во временный файл
	var bufferReader = bufio.NewReaderSize(r, 4<<10)
	// пытаемся определить тип содержимого
	data, err := bufferReader.Peek(512) // читаем первые 512 байт файла
	if err != nil && err != io.EOF {
		tmpfile.Close()
		err = &os.PathError{Op: "create", Path: tmpfileName, Err: err}
		return nil, err
	}
	var mimetype = http.DetectContentType(data) // определяем тип содержимого
	// одновременно с сохранением в файл считаем две хеш-суммы
	var crc32, md5 = crc32.NewIEEE(), md5.New()
	size, err := bufferReader.WriteTo(io.MultiWriter(tmpfile, crc32, md5))
	if err != nil {
		tmpfile.Close()
		err = &os.PathError{Op: "write", Path: tmpfileName, Err: err}
		return nil, err
	}
	// формируем информацию о файле
	data = md5.Sum(nil)
	var fi = &FileInfo{
		Name: base64.RawURLEncoding.EncodeToString(
			append(crc32.Sum(nil), data...)),
		Mimetype: mimetype,
		Size:     size,
		CRC32:    crc32.Sum32(),
		MD5:      hex.EncodeToString(data),
	}
	// закрываем временный файл
	if err = tmpfile.Close(); err != nil {
		if e, ok := err.(*os.PathError); ok {
			e.Path = tmpfileName
		}
		return nil, err
	}
	// объединяем имя файла с корневым каталогом
	var name = filepath.Join(root, fi.Name[:1], fi.Name[1:3], fi.Name[3:])
	// если файл уже существует, то просто обновляем его время создания
	var now = time.Now()
	if err = os.Chtimes(name, now, now); err == nil {
		// возвращаем информацию о файле, временный файл будет автоматически
		// удален
		return fi, nil
	}
	// если такого файла нет, то создаем для него каталог
	if err = os.MkdirAll(filepath.Dir(name), 0700); err != nil {
		err.(*os.PathError).Path = fi.Name
		return nil, err
	}
	// перемещаем временный файл в этот каталог
	if err = os.Rename(tmpfile.Name(), name); err != nil {
		err.(*os.PathError).Path = fi.Name
		return nil, err
	}
	// возвращаем информацию о созданном файле
	return fi, nil
}

// tmpfileName используется в качестве имени временного файла при генериции
// ошибок
const tmpfileName = "<temporary file>"

// Open открывает файл из каталога.
func (s *Store) Open(prefix, name string) (*os.File, error) {
	if len(name) < 27 {
		return nil, ErrNotFound
	}
	// полное имя для доступа к файлу
	var fullName = filepath.Join(s.root, prefix, name[:1], name[1:3], name[3:])
	file, err := os.Open(fullName) // открываем файл
	if err != nil {
		err.(*os.PathError).Path = name
		return nil, err
	}
	// получаем информацю о нем и проверяем, что это не каталог
	fi, err := file.Stat()
	if err != nil {
		file.Close()
		err.(*os.PathError).Path = name
		return nil, err
	}
	// возвращаем ошибку, если это каталог, а не файл
	if fi.IsDir() {
		file.Close()
		return nil, &os.PathError{Op: "open", Path: name, Err: os.ErrPermission}
	}
	// обновляем время доступа к файлу
	var now = time.Now()
	os.Chtimes(fullName, now, now)
	return file, nil // возвращаем открытый файл
}

// Remove удаляет файл из хранилища.
func (s *Store) Remove(prefix, name string) error {
	if len(name) < 27 {
		return ErrNotFound
	}
	var fullName = filepath.Join(s.root, prefix, name[:1], name[1:3], name[3:])
	if err := os.Remove(fullName); err != nil {
		err.(*os.PathError).Path = name
		return err
	}
	// пытаемся удалить пустые каталоги, если они образовались
	for i := 0; i < 2; i++ {
		fullName = filepath.Dir(fullName)
		if err := os.Remove(fullName); err != nil {
			break // если не получилось, значит каталог не пустой
		}
	}
	return nil
}

// Clean удаляет старые файлы, к которым не обращались больше заданного времени.
func (s *Store) Clean(lifetime time.Duration) error {
	// удаляем вообще все файлы, если время жизни не задано
	if lifetime <= 0 {
		return os.RemoveAll(s.root)
	}
	// вычисляем крайнюю дату валидности файлов
	var valid = time.Now().Add(-lifetime)
	var err = filepath.Walk(s.root,
		func(filename string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			// не удаляем каталоги и новые файлы
			if info.IsDir() || info.ModTime().After(valid) {
				return nil
			}
			// удаляем старый файл
			if err = os.Remove(filename); err != nil {
				return nil // ошибку удаления игнорируем
			}
			// log.Debug("old file deleted", "filename", filename)
			// пытаемся удалить пустые каталоги
			for i := 0; i < 2; i++ {
				filename = filepath.Dir(filename)
				if err = os.Remove(filename); err != nil {
					break // каталог не пустой
				}
			}
			return nil
		})
	if os.IsNotExist(err) {
		return nil // игнорируем ошибку, что файл не существует
	}
	return err
}
