//
//  File :     classad_binary_op_unwind.h
//
//
//  Author :   Francesco Prelz ($Author: drebatto $)
//  e-mail :   "francesco.prelz@mi.infn.it"
//
//  Revision history :
//   2-Mar-2006 Original release
//
//  Description:
//   Classad Unparser spaecialisation to extract the arguments
//   of binary operations of a given ExprTree.
//
//  Copyright (c) 2004 Istituto Nazionale di Fisica Nucleare (INFN).
//   All rights reserved.
//   See http://grid.infn.it/grid/license.html for license details.

#include "classad_distribution.h"

#ifndef __CLASSAD_BINARY_OP_UNWIND_H__
#define __CLASSAD_BINARY_OP_UNWIND_H__

#ifdef WANT_NAMESPACES
using namespace classad;

namespace classad {
#endif

class BinaryOpUnwind : public ClassAdUnParser
{
    private:
        // Counter for values appearing in each attribute inspected by 'member' function
        std::map<std::string, std::size_t> m_member_list_counter_;
    public:
          /// Constructor
        BinaryOpUnwind( );
          /// Destructor
        virtual ~BinaryOpUnwind( );
          /// Operator unparser
        virtual void UnparseAux( std::string &buffer, Operation::OpKind op, 
                                        ExprTree *op1, ExprTree *op2, ExprTree *op3 );
        virtual void UnparseAux( std::string &buffer, std::string &fnName, std::vector<ExprTree*>& args );


        std::vector<std::string> m_unwind_output;
};

#ifdef WANT_NAMESPACES
} // end of classad namespace
#endif

#endif // defined __CLASSAD_BINARY_OP_UNWIND_H__
