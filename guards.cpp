#include "guards.h"
#include "lfht.h"

#include <limits>

namespace NLFHT {
    const AtomicBase TBaseGuard::NO_TABLE = std::numeric_limits<AtomicBase>::max();

    void TThreadGuardTable::RegisterTable(TGuardable* pTable) {
        if (!GuardTable) {
           GuardTable = new TGuardTable;
        }
        TBaseGuard* guard = pTable->AcquireGuard();
        assert(guard);
        guard->ThreadId = CurrentThreadId();
        assert(GuardTable->find(pTable) == GuardTable->end());
        (*GuardTable)[pTable] = guard;
    }

    void TThreadGuardTable::ForgetTable(TGuardable* pTable) {
        TGuardTable::iterator it = GuardTable->find(pTable);
        it->second->Release();
        GuardTable->erase(it);

        if (GuardTable->empty()) {
            delete (TGuardTable*)GuardTable;
            GuardTable = (TGuardTable*)(0);
        }
    }

    NLFHT_THREAD_LOCAL TThreadGuardTable::TGuardTable *TThreadGuardTable::GuardTable = 0;

    TBaseGuard::TBaseGuard(TBaseGuardManager* parent)
        : Next(0)
        , Parent(parent)
        , AliveCnt(0)
        , KeyCnt(0)
        , ThreadId(size_t(-1))
    {
        Init();
#ifndef NDEBUG
        AtomicIncrement(Parent->GuardsCreated);
#endif
    }

    TBaseGuard::~TBaseGuard() {
        assert(ThreadId == size_t(-1));
#ifndef NDEBUG
        AtomicIncrement(Parent->GuardsDeleted);
#endif
    }

    void TBaseGuard::Init() {
        AliveCnt = 0;
        KeyCnt = 0;

        GuardedTable = NO_TABLE;
        PTDLock = false;

#ifndef NDEBUG
        // JUST TO DEBUG
        LocalPutCnt = 0;
        LocalCopyCnt = 0;
        LocalDeleteCnt = 0;
        LocalLookUpCnt = 0;
        GlobalPutCnt = 0;
        GlobalGetCnt = 0;
#endif
        ThreadId = (size_t)-1;
    }

    void TBaseGuard::Release() {
#ifdef TRACE_MEM
        Cerr << "Release " << (size_t)(this) << '\n';
#endif
        AtomicAdd(Parent->KeyCnt, KeyCnt);
        AtomicAdd(Parent->AliveCnt, AliveCnt);
        Init();
    }

    TBaseGuardManager::TBaseGuardManager()
        : Head(this)
        , AliveCnt(0)
        , KeyCnt(0)
#ifndef NDEBUG
        , GuardsCreated(0)
        , GuardsDeleted(0)
#endif
    {
    }

    TBaseGuard* TBaseGuardManager::AcquireGuard() {
        for (TBaseGuard* current = Head; current; current = current->Next)
            if (current->ThreadId == size_t(-1)) {
                size_t id = CurrentThreadId();
                if (AtomicCas((Atomic*)&current->ThreadId, id, size_t(-1))) {
#ifdef TRACE_MEM
                    Cerr << "Acquire " << (size_t)current << '\n';
#endif
                    return current;
                }
            }
        return CreateGuard();
    }

    size_t TBaseGuardManager::GetFirstGuardedTable() {
        size_t result = TBaseGuard::NO_TABLE;
        for (TBaseGuard* current = Head; current; current = current->Next)
            if (current->ThreadId != size_t(-1))
                result = Min(result, (size_t)current->GuardedTable);
        return result;
    }

    AtomicBase TBaseGuardManager::TotalAliveCnt() {
        AtomicBase result = AliveCnt;
        for (TBaseGuard* current = Head; current; current = current->Next)
            result += current->AliveCnt;
        return result;
    }

    AtomicBase TBaseGuardManager::TotalKeyCnt() {
        AtomicBase result = KeyCnt;
        for (TBaseGuard* current = Head; current; current = current->Next)
            result += current->KeyCnt;
        return result;
    }

    void TBaseGuardManager::ZeroKeyCnt() {
        for (TBaseGuard* current = Head; current; current = current->Next)
            current->KeyCnt = 0;
        KeyCnt = 0;
    }

    bool TBaseGuardManager::CanPrepareToDelete() {
        for (TBaseGuard* current = Head; current; current = current->Next)
            if (current->PTDLock)
                return false;
        return true;
    }

    // JUST TO DEBUG

    std::string TBaseGuard::ToString() {
        std::stringstream tmp;
        tmp << "TGuard " << '\n'
            << "KeyCnt " << KeyCnt << '\n'
            << "AliveCnt " << AliveCnt << '\n';
        return tmp.str();
    }

    std::string TBaseGuardManager::ToString() {
        std::stringstream tmp;
        tmp << "GuardManager --------------\n";
        for (TBaseGuard* current = Head; current; current = current->Next)
            tmp << current->ToString();
        tmp << "Common KeyCnt " << KeyCnt << '\n'
            << "Common AliveCnt " << AliveCnt << '\n';
        return tmp.str();
    }

    void TBaseGuardManager::PrintStatistics(std::ostream& str) {
#ifndef NDEBUG
        size_t localPutCnt = 0;
        size_t localCopyCnt = 0;
        size_t localDeleteCnt = 0;
        size_t localLookUpCnt = 0;
        size_t globalPutCnt = 0;
        size_t globalGetCnt = 0;
        for (TBaseGuard* current = Head; current; current = current->Next) {
            localPutCnt += current->LocalPutCnt;
            localCopyCnt += current->LocalCopyCnt;
            localDeleteCnt += current->LocalDeleteCnt;
            localLookUpCnt += current->LocalLookUpCnt;
            globalGetCnt += current->GlobalGetCnt;
            globalPutCnt += current->GlobalPutCnt;
        }

        str << "LocalPutCnt " << localPutCnt << '\n'
            << "LocalDeleteCnt " << localDeleteCnt << '\n'
            << "LocalCopyCnt " << localCopyCnt << '\n'
            << "LocalLookUpCnt " << localLookUpCnt << '\n'
            << "GlobalPutCnt " << globalPutCnt << '\n'
            << "GlobalGetCnt " << globalGetCnt << '\n';
#endif
    }

    TBaseGuard* TBaseGuardManager::CreateGuard() {
        TBaseGuard* guard = NewGuard();
#ifdef TRACE_MEM
        Cerr << "CreateGuard " << (size_t)guard << '\n';
#endif
        guard->ThreadId = CurrentThreadId();
        while (true) {
            guard->Next = Head;
            if (AtomicCas(&Head, guard, guard->Next))
               break;
        }
        return guard;
    }
};
