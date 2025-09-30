pub mod gurobi;

#[derive(Debug, PartialEq, Eq)]
pub enum VarStatus {
    Basic,
    NonbasicLower,
    NonbasicUpper,
    Superbasic,
}

#[derive(Debug, PartialEq, Eq)]
pub enum LPStatus {
    OPTIMAL,
    INFEASIBLE,
    UNBOUNDED,
    UNKNOWN,
}

pub enum LPConstrType {
    EQ,
    LEQ,
    GEQ,
}

pub type Flt = f64;

pub trait LinearProgram {
    type Constr : Clone;
    type Var;

    fn inf() -> Flt;

    fn create() -> Self;

    fn reset(&mut self);

    fn set_minimize(&mut self);

    fn get_objval(&self) -> Flt;

    fn optimize(&mut self);

    fn get_status(&self) -> LPStatus;

    // CONSTRAINTS

    fn add_constr_with_rhs(&mut self, constr_type: LPConstrType, rhs: Flt) -> Self::Constr;

    fn set_constr_bound(&mut self, constr: &Self::Constr, constr_type: LPConstrType, rhs: Flt);

    fn get_coeff(&self, constr: &Self::Constr, var: &Self::Var) -> Flt;

    fn set_coeff(&mut self, constr: &Self::Constr, var: &Self::Var, coeff: Flt);

    fn get_constr_dual_val(&self, constr: &Self::Constr) -> Flt;

    // VARIABLES

    fn add_var(
        &mut self,
        lower: Flt,
        upper: Flt,
        obj: Flt,
        colconstrs: &[Self::Constr],
        colvals: &[Flt],
    ) -> Self::Var;

    fn remove_var(&mut self, var: Self::Var);

    fn get_var_val(&self, var: &Self::Var) -> Flt;

    fn get_var_status(&self, var: &Self::Var) -> VarStatus;

    fn set_var_obj(&mut self, var: &Self::Var, obj: Flt);

    fn set_var_bound(&mut self, var: &Self::Var, lower: Flt, upper: Flt);

    fn get_var_reduced_cost(&self, var: &Self::Var) -> Flt;

    fn print(&self);
}
