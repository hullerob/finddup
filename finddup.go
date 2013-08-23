// Copyright 2013, Robert Hulle. Use of this source code is governed
// by ISC license that can be found in the LICENSE file.

// This program finds files with identical size and hash. It searches
// recursively through one or more directories.
package main

import (
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"io"
	"os"
)

type file struct {
	name string
	size int64
	md5  string
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: finddup <directory> ...")
		os.Exit(1)
	}
	files := make(chan file)
	go func() {
		for _, dir := range os.Args[1:] {
			findFiles(dir, files)
		}
		close(files)
	}()
	sizeClusters := make(chan []file)
	go findSize(files, sizeClusters)
	md5Clusters := make(chan []file)
	go findMd5(sizeClusters, md5Clusters)
	printClusters(md5Clusters)
}

func getFiles(dir string) ([]os.FileInfo, error) {
	d, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	defer d.Close()
	return d.Readdir(-1)
}

func findFiles(root string, files chan<- file) {
	finfo, err := getFiles(root)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error reading directory '%s': %v\n",
			root, err)
	}
	for _, f := range finfo {
		if f.IsDir() {
			findFiles(root+"/"+f.Name(), files)
		} else {
			files <- file{name: root + "/" + f.Name(),
				size: f.Size()}
		}
	}
}

func findSize(files <-chan file, clusters chan<- []file) {
	sizes := make(map[int64][]file)
	for f := range files {
		sizes[f.size] = append(sizes[f.size], f)
	}
	for _, v := range sizes {
		clusters <- v
	}
	close(clusters)
}

func md5File(name string) (string, error) {
	file, err := os.Open(name)
	if err != nil {
		return "", err
	}
	defer file.Close()
	h := md5.New()
	_, err = io.Copy(h, file)
	if err != nil {
		return "", err
	}
	out := make([]byte, base64.StdEncoding.EncodedLen(h.Size()))
	base64.StdEncoding.Encode(out, h.Sum(nil))
	return string(out), nil
}

func clusterMd5(files []file) (clusters [][]file) {
	hashes := make(map[string][]file)
	for _, f := range files {
		var err error
		f.md5, err = md5File(f.name)
		if err == nil {
			hashes[f.md5] = append(hashes[f.md5], f)
		} else {
			fmt.Fprintf(os.Stderr, "can not hash file '%s': %v\n",
				f.name, err)
		}
	}
	for _, v := range hashes {
		clusters = append(clusters, v)
	}
	return
}

func findMd5(sizeCl <-chan []file, hashCl chan<- []file) {
	for cluster := range sizeCl {
		hashes := clusterMd5(cluster)
		for _, v := range hashes {
			hashCl <- v
		}
	}
	close(hashCl)
}

func printClusters(clusters <-chan []file) {
	var size int64
	for c := range clusters {
		if len(c) == 1 {
			continue
		}
		for _, file := range c {
			fmt.Println(file.name)
		}
		fmt.Println("")
		size += c[0].size * (int64(len(c)) - 1)
	}
	fmt.Fprintf(os.Stderr, "duplicated size: %d B\n", size)
}
