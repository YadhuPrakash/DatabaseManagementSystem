#include "pfm.h"

#include <fstream>
#include <iostream>
#include <stdio.h>
#include <cstddef>
#include <sys/types.h>
#include <sys/stat.h>
#include <math.h>

PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
}


PagedFileManager::~PagedFileManager()
{
}


RC PagedFileManager::createFile(const string &fileName)
{
	struct stat stFileInfo;
	if(stat(fileName.c_str(), &stFileInfo) == 0)
	{
		return -1;
	}

	ofstream outfile;
	outfile.open(fileName.c_str(), ios::binary | ios::out);
	outfile.close();
	return 0;
}

RC PagedFileManager::checkIfFileExists(const string &fileName)
{
	struct stat stFileInfo;
	if(stat(fileName.c_str(), &stFileInfo) == 0)
	{
		return 0;
	}
	return -1;
}

RC PagedFileManager::destroyFile(const string &fileName)
{
	struct stat stFileInfo;
	if(stat(fileName.c_str(), &stFileInfo) == -1)
	{
		return -1;
	}
	int removedResult = remove(fileName.c_str());
    if(removedResult == 0)
    {
    	return 0;
    }
    return -1;
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
	struct stat stFileInfo;

	if(fileHandle.file != 0)		//using an already used file handle is not allowed
	{
		return -1;
	}
	if(stat(fileName.c_str(), &stFileInfo) == 0) // if file exists
	{
        fstream* fileStream = new fstream(fileName.c_str(), fstream::in | fstream::binary | fstream::out );
		fileHandle.setFile(fileStream);
	    return 0;
	}
	return -1;
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
	fileHandle.file->flush();
	fileHandle.file->close();
	delete fileHandle.file;
	fileHandle.file = NULL;
	return 0;
}


FileHandle::FileHandle()
{
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
    file = NULL;
}


FileHandle::~FileHandle()
{
}


RC FileHandle::readPage(PageNum pageNum, void *data)
{
	long readPosInFile = pageNum * PAGE_SIZE;
	file->seekg(0, file->end);
	int endOfFilePos = file->tellg();
	if(endOfFilePos <= readPosInFile)
	{
		return -1;
	}
	file->seekg(readPosInFile, file->beg);
	file->read((char*)(data), PAGE_SIZE);
	readPageCounter = readPageCounter + 1;
    return 0;
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
	unsigned totalPages = getNumberOfPages();
	if(pageNum > totalPages - 1)			//no need to check if greater than 0, since it is unsigned
	{
	    return -1;
	}
	long posToWrite = pageNum * PAGE_SIZE;
	file->seekg(posToWrite, file->beg);
	file->write((char *)(data), PAGE_SIZE);
	file->flush();
	writePageCounter = writePageCounter + 1;
	return 0;
}


RC FileHandle::appendPage(const void *data)
{
	file->seekg(0, file->end);
	file->write((char*)(data), PAGE_SIZE);
	file->flush();
	appendPageCounter = appendPageCounter + 1;
    return 0;
}

void FileHandle::setFile(fstream *fs)
{
	file = fs;
}


unsigned FileHandle::getNumberOfPages()
{
	file->seekg(0, file->end);
	int fileSize = file->tellg();
	unsigned numberOfPages = ceil(fileSize * 1.0/PAGE_SIZE);
	return numberOfPages;
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	readPageCount = readPageCounter;
	writePageCount = writePageCounter;
	appendPageCount = appendPageCounter;
	return 0;
}
