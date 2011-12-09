#pragma once

#include <hash_map>

#include "atomic.h"

template<typename Key, typename Value>
class MutexHashTable
{
private:
    __gnu_cxx::hash_map<Key, Value> m_data;
    mutable Mutex m_mutex;

public:
    MutexHashTable(size_t initialSize = 1)
        : m_data(initialSize)
    {

    }

    bool Find(const Key& key, Value&) const
    {
        Guard<Mutex> g(m_mutex);
        return m_data.find(key) != m_data.end();
    }

    size_t Size() const
    {
        Guard<Mutex> g(m_mutex);
        return m_data.size();
    }

    void Delete(const Key& key)
    {
        Guard<Mutex> g(m_mutex);
        m_data.erase(key);
    }

    void PutIfAbsent(const Key& key, const Value& value)
    {
        Guard<Mutex> g(m_mutex);
        if (m_data.find(key) == m_data.end())
        {
            m_data.insert( std::make_pair(key, value) );
        }
    }
};
