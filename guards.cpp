#include "guards.h"
#include "lfht.h"

#include <limits>

namespace NLFHT {
    const AtomicBase BaseGuard::NO_TABLE = std::numeric_limits<AtomicBase>::max();

    void ThreadGuardTable::RegisterTable(Guardable* pTable) {
        if (!m_GuardTable) {
           m_GuardTable = new GuardTable;
        }
        BaseGuard* guard = pTable->AcquireGuard();
        assert(guard);
        guard->m_ThreadId = CurrentThreadId();
        assert(m_GuardTable->find(pTable) == m_GuardTable->end());
        (*m_GuardTable)[pTable] = guard;
    }

    void ThreadGuardTable::ForgetTable(Guardable* pTable) {
        GuardTable::iterator it = m_GuardTable->find(pTable);
        it->second->Release();
        m_GuardTable->erase(it);

        if (m_GuardTable->empty()) {
            delete (GuardTable*)m_GuardTable;
            m_GuardTable = (GuardTable*)(0);
        }
    }

    NLFHT_THREAD_LOCAL ThreadGuardTable::GuardTable *ThreadGuardTable::m_GuardTable = 0;

    BaseGuard::BaseGuard(BaseGuardManager* parent)
        : Next(0)
        , m_Parent(parent)
        , m_AliveCnt(0)
        , m_KeyCnt(0)
        , m_ThreadId(size_t(-1))
    {
        Init();
#ifndef NDEBUG
        AtomicIncrement(m_Parent->m_GuardsCreated);
#endif
    }

    BaseGuard::~BaseGuard()
    {
        assert(m_ThreadId == size_t(-1));
#ifndef NDEBUG
        AtomicIncrement(m_Parent->m_GuardsDeleted);
#endif
    }

    void BaseGuard::Init()
    {
        m_AliveCnt = 0;
        m_KeyCnt = 0;

        m_GuardedTable = NO_TABLE;
        m_PTDLock = false;

#ifndef NDEBUG
        // JUST TO DEBUG
        m_LocalPutCnt = 0;
        m_LocalCopyCnt = 0;
        m_LocalDeleteCnt = 0;
        m_LocalLookUpCnt = 0;
        m_GlobalPutCnt = 0;
        m_GlobalGetCnt = 0;
#endif
        m_ThreadId = (size_t)-1;
    }

    void BaseGuard::Release() {
#ifdef TRACE_MEM
        Cerr << "Release " << (size_t)(this) << '\n';
#endif
        AtomicAdd(m_Parent->m_KeyCnt, m_KeyCnt);
        AtomicAdd(m_Parent->m_AliveCnt, m_AliveCnt);
        Init();
    }

    BaseGuardManager::BaseGuardManager()
        : m_Head(this)
        , m_AliveCnt(0)
        , m_KeyCnt(0)
#ifndef NDEBUG
        , m_GuardsCreated(0)
        , m_GuardsDeleted(0)
#endif
    {
    }

    BaseGuard* BaseGuardManager::AcquireGuard() {
        for (BaseGuard* current = m_Head; current; current = current->Next)
            if (current->m_ThreadId == size_t(-1)) {
                size_t id = CurrentThreadId();
                if (AtomicCas((Atomic*)&current->m_ThreadId, id, size_t(-1))) {
#ifdef TRACE_MEM
                    Cerr << "Acquire " << (size_t)current << '\n';
#endif
                    return current;
                }
            }
        return CreateGuard();
    }

    size_t BaseGuardManager::GetFirstGuardedTable() {
        size_t result = BaseGuard::NO_TABLE;
        for (BaseGuard* current = m_Head; current; current = current->Next)
            if (current->m_ThreadId != size_t(-1))
                result = Min(result, (size_t)current->m_GuardedTable);
        return result;
    }

    AtomicBase BaseGuardManager::TotalAliveCnt() {
        AtomicBase result = m_AliveCnt;
        for (BaseGuard* current = m_Head; current; current = current->Next)
            result += current->m_AliveCnt;
        return result;
    }

    AtomicBase BaseGuardManager::TotalKeyCnt()
    {
        AtomicBase result = m_KeyCnt;
        for (BaseGuard* current = m_Head; current; current = current->Next)
            result += current->m_KeyCnt;
        return result;
    }

    void BaseGuardManager::ZeroKeyCnt()
    {
        for (BaseGuard* current = m_Head; current; current = current->Next)
            current->m_KeyCnt = 0;
        m_KeyCnt = 0;
    }

    bool BaseGuardManager::CanPrepareToDelete()
    {
        for (BaseGuard* current = m_Head; current; current = current->Next)
            if (current->m_PTDLock)
                return false;
        return true;
    }

    // JUST TO DEBUG

    std::string BaseGuard::ToString()
    {
        std::stringstream tmp;
        tmp << "TGuard " << '\n'
            << "KeyCnt " << m_KeyCnt << '\n'
            << "AliveCnt " << m_AliveCnt << '\n';
        return tmp.str();
    }

    std::string BaseGuardManager::ToString()
    {
        std::stringstream tmp;
        tmp << "GuardManager --------------\n";
        for (BaseGuard* current = m_Head; current; current = current->Next)
            tmp << current->ToString();
        tmp << "Common KeyCnt " << m_KeyCnt << '\n'
            << "Common AliveCnt " << m_AliveCnt << '\n';
        return tmp.str();
    }

    void BaseGuardManager::PrintStatistics(std::ostream& str)
    {
#ifndef NDEBUG
        size_t localPutCnt = 0;
        size_t localCopyCnt = 0;
        size_t localDeleteCnt = 0;
        size_t localLookUpCnt = 0;
        size_t globalPutCnt = 0;
        size_t globalGetCnt = 0;
        for (BaseGuard* current = m_Head; current; current = current->Next) {
            localPutCnt += current->m_LocalPutCnt;
            localCopyCnt += current->m_LocalCopyCnt;
            localDeleteCnt += current->m_LocalDeleteCnt;
            localLookUpCnt += current->m_LocalLookUpCnt;
            globalGetCnt += current->m_GlobalGetCnt;
            globalPutCnt += current->m_GlobalPutCnt;
        }

        str << "LocalPutCnt " << localPutCnt << '\n'
            << "LocalDeleteCnt " << localDeleteCnt << '\n'
            << "LocalCopyCnt " << localCopyCnt << '\n'
            << "LocalLookUpCnt " << localLookUpCnt << '\n'
            << "GlobalPutCnt " << globalPutCnt << '\n'
            << "GlobalGetCnt " << globalGetCnt << '\n';
#endif
    }

    BaseGuard* BaseGuardManager::CreateGuard()
    {
        BaseGuard* guard = NewGuard();
#ifdef TRACE_MEM
        Cerr << "CreateGuard " << (size_t)guard << '\n';
#endif
        guard->m_ThreadId = CurrentThreadId();
        while (true) {
            guard->Next = m_Head;
            if (AtomicCas(&m_Head, guard, guard->Next))
               break;
        }
        return guard;
    }
};
