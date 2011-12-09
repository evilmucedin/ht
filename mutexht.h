#pragma once

#include <hash_map>

template<typename Key, typename Value>
class MutexHashTable
{
private:
    __gnu_cxx::hash_map<Key, Value> m_data;

public:
    MutexHashTable(size_t initialSize = 1)
        : m_data(initialSize)
    {

    }

    bool Find(const Key& key, Value&) const
    {
        return m_data.find(key) != m_data.end();
    }

    size_t Size() const
    {
        return m_data.size();
    }

    void Delete(const Key& key)
    {
        m_data.erase(key);
    }

    void PutIfAbsent(const Key& key, const Value& value)
    {
        if (m_data.find(key) == m_data.end())
        {
            m_data.insert( std::make_pair(key, value) );
        }
    }
};
