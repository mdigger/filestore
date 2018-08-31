package filestore

import (
	"net/http"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/mdigger/rest"
)

// ErrNotFound возвращается, если файл с таким именем не найден.
var ErrNotFound = rest.ErrNotFound

// Get отдает файл из хранилища.
func (s *Store) Get(c *rest.Context) error {
	// формируем имя файла в хранилище
	var (
		name = filepath.FromSlash(c.Param("filename"))
		ext  string // расширение файла в дополнительной необязательной чати пути
	)
	// после пути можно добавлять любые другие части - игнорируем их
	if indx := strings.IndexByte(name, filepath.Separator); indx >= 0 {
		name, ext = name[:indx], filepath.Ext(name[indx:])
	}
	// открываем файл
	file, err := s.Open("", name)
	if err != nil {
		return err
	}
	// отдаем содержимое файла в ответ
	// в качестве имени может использоваться расширение файла, имя которого
	// было передано в запросе в дополнительном пути
	// время не используем, т.к. все файлы уникальны в силу хешей и не могут
	// быть модифицированы без изменения имени
	err = c.ServeContent(ext, time.Time{}, file)
	file.Close()
	return err
}

// Post сохраняет файл в хранилище.
func (s *Store) Post(c *rest.Context) error {
	// открываем файл
	info, err := s.Create("", c.Request.Body)
	if err != nil {
		return err
	}
	// объединяем путь к созданному файлу с текущим путем запроса
	info.Name = path.Join(c.Request.URL.Path, info.Name)
	// отдаем ответ с информацией о файле
	c.SetHeader("Location", info.Name)
	c.SetStatus(http.StatusCreated)
	return c.Write(info)
}
