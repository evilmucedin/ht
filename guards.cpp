#include "guards.h"

#include <limits>

namespace NLFHT {
    const AtomicBase TGuard::NO_OWNER = 0;
    const AtomicBase TGuard::NO_TABLE = std::numeric_limits<AtomicBase>::max();

    void TThreadGuardTable::InitializePerThread() {
        GuardTable = new std::unordered_map<void*, TGuard*>(42);
    }

    void TThreadGuardTable::FinalizePerThread() {
        for (TGuardTable::iterator it = GuardTable->begin(); it != GuardTable->end(); it++)
            it->second->Release();
        delete GuardTable;
    }

    NLFHT_THREAD_LOCAL TThreadGuardTable::TGuardTable *TThreadGuardTable::GuardTable = 0;

    TGuard::TGuard() :
        Next(0),
        Owner(NO_OWNER),
        GuardedTable(NO_TABLE),
        PTDLock(false),
        LocalPutCnt(0),
        LocalCopyCnt(0),
        LocalDeleteCnt(0),
        LocalLookUpCnt(0),
        GlobalPutCnt(0),
        GlobalGetCnt(0),
        AliveCnt(0),
        KeyCnt(0)
    {
    }

    TGuard* TGuardManager::AcquireGuard(size_t owner) {
        for (TGuard* current = Head; current; current = current->Next)
            if (current->Owner == TGuard::NO_OWNER)
                if (AtomicCas(&current->Owner, owner, TGuard::NO_OWNER))
                    return current;
        return CreateGuard(owner);
    }

    size_t TGuardManager::GetFirstGuardedTable() {
        size_t result = TGuard::NO_TABLE;
        for (TGuard* current = Head; current; current = current->Next)
            if (current->Owner != TGuard::NO_OWNER)
                result = Min(result, (size_t)current->GuardedTable);
        return result;
    }

    AtomicBase TGuardManager::TotalAliveCnt() {
        AtomicBase result = 0;
        for (TGuard* current = Head; current; current = current->Next)
            result += current->AliveCnt;
        // TotalAliveCnt can be even negative in some cases,
        // cause summing a list is not atomic
        return result;
    }

    size_t TGuardManager::TotalKeyCnt() {
        size_t result = 0;
        for (TGuard* current = Head; current; current = current->Next)
            result += current->KeyCnt;
        return result;
    }

    void TGuardManager::ZeroKeyCnt() {
        for (TGuard* current = Head; current; current = current->Next)
            current->KeyCnt = 0;
    }

    bool TGuardManager::CanPrepareToDelete() {
        for (TGuard* current = Head; current; current = current->Next)
            if (current->PTDLock)
                return false;
        return true;            
    }

    // JUST TO DEBUG

    void TGuardManager::PrintStatistics(std::ostream& str) {
        size_t LocalPutCnt = 0, LocalCopyCnt = 0, LocalDeleteCnt = 0, LocalLookUpCnt = 0;
        size_t GlobalPutCnt = 0, GlobalGetCnt = 0;
        for (TGuard* current = Head; current; current = current->Next) {
            LocalPutCnt += current->LocalPutCnt;
            LocalCopyCnt += current->LocalCopyCnt;
            LocalDeleteCnt += current->LocalDeleteCnt;
            LocalLookUpCnt += current->LocalLookUpCnt;
            GlobalGetCnt += current->GlobalGetCnt;
            GlobalPutCnt += current->GlobalPutCnt;
            LocalPutCnt += current->LocalPutCnt;
        }

        str << "LocalPutCnt " << LocalPutCnt << '\n'
            << "LocalDeleteCnt " << LocalDeleteCnt << '\n'
            << "LocalCopyCnt " << LocalCopyCnt << '\n'
            << "LocalLookUpCnt " << LocalLookUpCnt << '\n'
            << "GlobalPutCnt " << GlobalPutCnt << '\n'
            << "GlobalGetCnt " << GlobalGetCnt << '\n';
    }

    TGuard* TGuardManager::CreateGuard(AtomicBase owner) {
        TGuard* guard = new TGuard;
        guard->Owner = owner;
        while (true) {
            guard->Next = Head;
            if (AtomicCas(&Head, guard, guard->Next))
               break;
        } 
        return guard;
    }
};
