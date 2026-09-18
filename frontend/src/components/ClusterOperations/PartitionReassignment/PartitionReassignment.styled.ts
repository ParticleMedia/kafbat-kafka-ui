import styled from 'styled-components';
import { Textarea } from 'components/common/Textbox/Textarea.styled';

export const Container = styled.div``;

export const Content = styled.div`
  display: grid;
  gap: 20px;
  padding: 0 24px 32px;
`;

export const Panel = styled.section`
  display: grid;
  gap: 12px;
  padding: 20px;
  border: 1px solid ${({ theme }) => theme.table.td.borderTop};
  border-radius: 8px;
  background: ${({ theme }) => theme.default.backgroundColor};
`;

export const Description = styled.p`
  margin: 0;
  color: ${({ theme }) => theme.table.th.color.normal};
`;

export const JsonInput = styled(Textarea)`
  min-height: 180px;
  resize: vertical;
  font-family: monospace;
  line-height: 1.5;
`;

export const Actions = styled.div`
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
`;

export const ModalActions = styled(Actions)`
  justify-content: flex-end;
`;

export const ModalBody = styled.div`
  display: grid;
  gap: 16px;
`;

export const ExecutionSummary = styled.ul`
  display: grid;
  gap: 8px;
  max-height: 220px;
  margin: 0;
  padding: 0;
  overflow: auto;
  list-style: none;

  li {
    display: flex;
    justify-content: space-between;
    gap: 16px;
    padding-bottom: 8px;
    border-bottom: 1px solid ${({ theme }) => theme.table.td.borderTop};
  }
`;

export const Field = styled.div`
  display: grid;
  gap: 6px;
`;

export const NumberInput = styled.input`
  width: 100%;
  height: 32px;
  padding: 0 12px;
  border: 1px solid ${({ theme }) => theme.input.borderColor.normal};
  border-radius: 4px;
  color: ${({ theme }) => theme.input.color.normal};
  background: ${({ theme }) => theme.input.backgroundColor.normal};
`;

export const ErrorMessage = styled.p`
  margin: 0;
  color: ${({ theme }) => theme.input.error};
`;

export const WarningMessage = styled.p`
  margin: 0;
  padding: 8px 12px;
  border-radius: 4px;
  color: ${({ theme }) => theme.table.th.color.normal};
  background: ${({ theme }) => theme.default.backgroundColor};
  border: 1px solid ${({ theme }) => theme.table.td.borderTop};
`;

export const OperationStatus = styled(WarningMessage)`
  width: fit-content;
`;

export const EmptyState = styled.p`
  margin: 0;
  padding: 20px 12px;
  text-align: center;
  color: ${({ theme }) => theme.table.th.color.normal};
`;

export const Progress = styled.div`
  display: grid;
  grid-template-columns: minmax(80px, 1fr) auto;
  align-items: center;
  gap: 8px;
  min-width: 140px;
`;

export const SuccessMessage = styled.p`
  margin: 0;
  color: ${({ theme }) => theme.tag.color};
  background: ${({ theme }) => theme.tag.backgroundColor.green};
  border-radius: 4px;
  padding: 8px 12px;
  width: fit-content;
`;

export const Hash = styled.code`
  overflow-wrap: anywhere;
`;

export const Table = styled.table`
  width: 100%;
  border-collapse: collapse;

  th,
  td {
    padding: 10px 12px;
    text-align: left;
    border-bottom: 1px solid ${({ theme }) => theme.table.td.borderTop};
  }
`;
