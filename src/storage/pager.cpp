#include "storage/pager.h"
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <stdexcept>

Pager::Pager(const std::string &filepath, size_t)
    : filepath_(filepath), page_count_(0)
{
    fd_ = open(filepath.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd_ < 0)
        throw std::runtime_error("Pager: cannot open " + filepath);
    struct stat st;
    fstat(fd_, &st);
    page_count_ = static_cast<uint32_t>(st.st_size / PAGE_SIZE);

    // Map existing whole chunks
    size_t num_chunks = (st.st_size + CHUNK_SIZE - 1) / CHUNK_SIZE;
    for (size_t i = 0; i < num_chunks; i++)
    {
        map_chunk(i);
    }
}

Pager::~Pager()
{
    flush_all();
    for (size_t i = 0; i < chunks_.size(); i++)
    {
        if (chunks_[i] != nullptr)
        {
            munmap(chunks_[i], CHUNK_SIZE);
        }
    }
    ftruncate(fd_, page_count_ * PAGE_SIZE);
    close(fd_);
}

void Pager::map_chunk(size_t chunk_idx)
{
    off_t offset = static_cast<off_t>(chunk_idx) * CHUNK_SIZE;
    struct stat st;
    fstat(fd_, &st);
    if (st.st_size < offset + (off_t)CHUNK_SIZE)
    {
        if (ftruncate(fd_, offset + CHUNK_SIZE) != 0)
            throw std::runtime_error("Pager: ftruncate failed");
    }
    void *ptr = mmap(NULL, CHUNK_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, offset);
    if (ptr == MAP_FAILED)
        throw std::runtime_error("Pager: mmap failed");
    if (chunk_idx >= chunks_.size())
    {
        chunks_.resize(chunk_idx + 1, nullptr);
    }
    chunks_[chunk_idx] = static_cast<uint8_t *>(ptr);
}

Page *Pager::fetch_page(uint32_t page_id)
{
    size_t chunk_idx = page_id / PAGES_PER_CHUNK;
    {
        std::shared_lock lk(mu_);
        if (chunk_idx < chunks_.size() && chunks_[chunk_idx] != nullptr)
        {
            size_t page_off = page_id % PAGES_PER_CHUNK;
            return reinterpret_cast<Page *>(chunks_[chunk_idx] + page_off * PAGE_SIZE);
        }
    }
    // Lazy map
    std::unique_lock lk(mu_);
    if (chunk_idx >= chunks_.size())
        chunks_.resize(chunk_idx + 1, nullptr);
    if (chunks_[chunk_idx] == nullptr)
        map_chunk(chunk_idx);
    size_t page_off = page_id % PAGES_PER_CHUNK;
    return reinterpret_cast<Page *>(chunks_[chunk_idx] + page_off * PAGE_SIZE);
}

uint32_t Pager::alloc_page()
{
    std::unique_lock lk(mu_);
    uint32_t pid = page_count_++;
    size_t chunk_idx = pid / PAGES_PER_CHUNK;
    if (chunk_idx >= chunks_.size())
    {
        map_chunk(chunk_idx);
    }
    return pid;
}

void Pager::flush_all()
{
    std::shared_lock lk(mu_);
    for (uint8_t *chunk : chunks_)
    {
        if (chunk != nullptr)
        {
            msync(chunk, CHUNK_SIZE, MS_SYNC);
        }
    }
}
