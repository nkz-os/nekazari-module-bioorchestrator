declare module '@nekazari/ui-kit' {
  import { ReactNode, ChangeEvent, ButtonHTMLAttributes, InputHTMLAttributes } from 'react';

  // ── Card ──
  export interface CardProps { padding?: 'sm' | 'md' | 'lg' | 'none'; className?: string; children?: ReactNode; }
  export const Card: React.FC<CardProps>;

  // ── Button ──
  export interface ButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
    variant?: 'primary' | 'secondary' | 'ghost' | 'danger';
    size?: 'sm' | 'md' | 'lg';
    loading?: boolean;
    leadingIcon?: ReactNode;
    trailingIcon?: ReactNode;
    children?: ReactNode;
  }
  export const Button: React.FC<ButtonProps>;

  // ── Badge ──
  export interface BadgeProps { intent?: 'default' | 'positive' | 'warning' | 'negative' | 'info'; size?: 'sm' | 'md'; className?: string; children?: ReactNode; }
  export const Badge: React.FC<BadgeProps>;

  // ── Spinner ──
  export interface SpinnerProps { size?: 'sm' | 'md' | 'lg'; className?: string; }
  export const Spinner: React.FC<SpinnerProps>;

  // ── Stack / Inline ──
  export interface StackProps { gap?: 'tight' | 'inline' | 'stack' | 'section'; align?: string; justify?: string; className?: string; children?: ReactNode; }
  export const Stack: React.FC<StackProps>;
  export const Inline: React.FC<StackProps & { wrap?: boolean }>;

  // ── Input ──
  export interface InputProps extends Omit<InputHTMLAttributes<HTMLInputElement>, 'size' | 'prefix'> {
    size?: 'sm' | 'md';
    error?: boolean;
    prefix?: ReactNode;
    suffix?: ReactNode;
  }
  export const Input: React.FC<InputProps>;

  // ── Select ──
  export interface SelectOption { value: string; label: string; disabled?: boolean; }
  export interface SelectProps {
    value?: string;
    onValueChange?: (value: string) => void;
    options?: SelectOption[];
    placeholder?: string;
    size?: 'sm' | 'md';
    error?: boolean;
    disabled?: boolean;
    className?: string;
  }
  export const Select: React.FC<SelectProps>;

  // ── Tabs ──
  export interface TabsRootProps { defaultValue?: string; value?: string; onValueChange?: (value: string) => void; children?: ReactNode; }
  export interface TabsListProps { children?: ReactNode; className?: string; }
  export interface TabsTriggerProps { value: string; count?: number; children?: ReactNode; }
  export interface TabsContentProps { value: string; children?: ReactNode; }

  export const Tabs: {
    Root: React.FC<TabsRootProps>;
    List: React.FC<TabsListProps>;
    Trigger: React.FC<TabsTriggerProps>;
    Content: React.FC<TabsContentProps>;
  };

  // ── Panel ──
  export interface PanelProps { variant?: 'glass' | 'solid' | 'opaque'; children?: ReactNode; className?: string; }
  export interface PanelHeaderProps { children?: ReactNode; className?: string; }
  export interface PanelTitleProps { children?: ReactNode; className?: string; }
  export interface PanelActionsProps { children?: ReactNode; className?: string; }
  export interface PanelBodyProps { children?: ReactNode; className?: string; }

  export const Panel: React.FC<PanelProps> & {
    Header: React.FC<PanelHeaderProps>;
    Title: React.FC<PanelTitleProps>;
    Actions: React.FC<PanelActionsProps>;
    Body: React.FC<PanelBodyProps>;
  };

  // ── Surface ──
  export interface SurfaceProps {
    variant?: 'default' | 'raised' | 'sunken';
    padding?: 'none' | 'tight' | 'inline' | 'stack' | 'section';
    radius?: 'none' | 'xs' | 'sm' | 'md' | 'lg' | 'xl' | '2xl';
    as?: React.ElementType;
    children?: ReactNode;
    className?: string;
  }
  export const Surface: React.FC<SurfaceProps>;

  // ── DataTable ──
  export interface ColumnDef<TData> {
    accessorKey?: string;
    header?: string;
    cell?: (info: { getValue: () => any; row: { original: TData } }) => ReactNode;
  }
  export interface DataTableProps<TData> {
    columns: ColumnDef<TData>[];
    data: TData[];
    sorting?: { id: string; desc: boolean }[];
    onSortingChange?: (sorting: { id: string; desc: boolean }[]) => void;
    columnFilters?: Record<string, any>;
    onColumnFiltersChange?: (filters: Record<string, any>) => void;
    onRowClick?: (row: TData) => void;
    density?: 'comfortable' | 'compact';
    emptyState?: ReactNode;
    className?: string;
  }
  export function DataTable<TData>(props: DataTableProps<TData>): React.ReactElement;

  // ── MetricCard / MetricGrid ──
  export interface MetricCardProps {
    label: string;
    value: string | number;
    unit?: string;
    trend?: { direction: 'up' | 'down' | 'neutral'; value: string };
    accentColor?: string;
    className?: string;
  }
  export const MetricCard: React.FC<MetricCardProps>;

  export interface MetricGridProps { columns?: 2 | 3 | 4 | 6; children?: ReactNode; className?: string; }
  export const MetricGrid: React.FC<MetricGridProps>;

  // ── EmptyState ──
  export interface EmptyStateProps { icon?: ReactNode; title: string; description?: string; action?: ReactNode; className?: string; }
  export const EmptyState: React.FC<EmptyStateProps>;

  // ── Skeleton ──
  export interface SkeletonProps { variant?: 'text' | 'circle' | 'rect'; width?: string; height?: string; className?: string; }
  export const Skeleton: React.FC<SkeletonProps>;

  // ── ProgressBar ──
  export interface ProgressBarProps { value: number; size?: 'sm' | 'md'; intent?: 'default' | 'positive' | 'warning' | 'negative'; showLabel?: boolean; className?: string; }
  export const ProgressBar: React.FC<ProgressBarProps>;

  // ── IconButton ──
  export interface IconButtonProps { 'aria-label': string; children?: ReactNode; variant?: 'ghost' | 'secondary'; size?: 'sm' | 'md'; active?: boolean; onClick?: () => void; }
  export const IconButton: React.FC<IconButtonProps>;

  // ── Form ──
  export interface FormGridProps { columns?: 1 | 2 | 3; children?: ReactNode; className?: string; }
  export const FormGrid: React.FC<FormGridProps>;

  export interface FormFieldProps { label: string; required?: boolean; description?: string; error?: string; span?: 1 | 2 | 3; children?: ReactNode; className?: string; }
  export const FormField: React.FC<FormFieldProps>;

  // ── DetailGrid / DetailItem ──
  export interface DetailGridProps { columns?: 1 | 2 | 3; children?: ReactNode; className?: string; }
  export const DetailGrid: React.FC<DetailGridProps>;

  export interface DetailItemProps { label: string; value: ReactNode; className?: string; }
  export const DetailItem: React.FC<DetailItemProps>;
}
